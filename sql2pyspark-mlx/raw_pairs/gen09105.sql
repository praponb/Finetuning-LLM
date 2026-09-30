with filtered as (
    select *, concat_ws('/', status, method) status_method_key
    from payments
    where amount >= 1410
),
enriched as (
    select
        *,
        min(amount) over (partition by method order by amount, id rows between unbounded preceding and current row) running_min_amount,
        avg(amount) over (partition by method order by amount, id rows between unbounded preceding and current row) running_avg_amount,
        max(amount) over (partition by method order by amount, id rows between unbounded preceding and current row) running_max_amount
    from filtered
),
base as (
    select
        id,
        order_id,
        method,
        amount,
        running_min_amount,
        running_avg_amount,
        running_max_amount,
        cast(running_min_amount as int) running_min_amount_int,
        round(running_max_amount * 1.07, 2) adjusted_running_max_amount,
        coalesce(method, 'NONE') method_filled
    from enriched
    where paid_at > to_timestamp('2023-06-22 00:00:00')
      and order_id is not null
),
metrics as (
    select
        id,
        order_id,
        method,
        running_min_amount,
        ntile(4) over (partition by method order by amount, id) amount_bucket,
        row_number() over (partition by order_id order by running_max_amount desc, id) seq_no,
        avg(adjusted_running_max_amount) over (partition by order_id order by running_min_amount_int, id rows between 6 preceding and 1 preceding) adjusted_running_max_amount_avg_6_prior,
        round(running_max_amount / sum(running_max_amount) over (partition by order_id), 4) running_max_amount_share
    from base
    where amount > 1400
),
scored as (
    select
        id,
        running_min_amount,
        running_max_amount_share,
        avg(running_max_amount_share) over (partition by order_id, method order by running_min_amount, id rows between unbounded preceding and current row) running_avg_running_max_amount_share,
        sum(running_max_amount_share) over (partition by order_id, method order by running_min_amount, id rows between unbounded preceding and current row) running_sum_running_max_amount_share,
        min(running_max_amount_share) over (partition by order_id, method order by running_min_amount, id rows between unbounded preceding and current row) running_min_running_max_amount_share,
        row_number() over (partition by method order by seq_no desc, id) row_seq
    from metrics
),
scored_payments as (
    select
        id,
        running_sum_running_max_amount_share,
        running_min_running_max_amount_share,
        percent_rank() over (order by running_sum_running_max_amount_share desc nulls last) running_sum_running_max_amount_share_percent_rank,
        lead(running_min_running_max_amount_share, 1) over (order by running_min_running_max_amount_share, id) next_running_min_running_max_amount_share,
        ntile(5) over (order by row_seq, id) row_seq_bucket,
        sum(running_avg_running_max_amount_share) over (order by running_sum_running_max_amount_share, id rows between unbounded preceding and current row) cumulative_running_avg_running_max_amount_share
    from scored
)
select
    case when next_running_min_running_max_amount_share >= 0.64 then 'HIGH' else 'LOW' end as next_running_min_running_max_amount_share_tier,
    percentile_approx(running_sum_running_max_amount_share, array(0.25, 0.5, 0.75)) as running_sum_running_max_amount_share_quartiles,
    count(distinct id) as distinct_ids
from scored_payments
group by 1
order by next_running_min_running_max_amount_share_tier nulls last
