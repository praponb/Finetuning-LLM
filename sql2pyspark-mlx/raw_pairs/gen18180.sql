with enriched as (
    select src.*, c.signup_date
    from subscriptions src
    left join customers c
        on src.customer_id = c.id
),
enriched_subscriptions as (
    select
        *,
        sum(monthly_fee) over (partition by date_trunc('week', end_date), plan order by signup_date, id rows between unbounded preceding and current row) as cumulative_monthly_fee,
        row_number() over (partition by date_trunc('MONTH', start_date), plan order by end_date, id) as row_seq
    from enriched
),
enriched_subscriptions_2 as (
    select
        *,
        percent_rank() over (partition by plan order by monthly_fee desc) as monthly_fee_percent_rank,
        row_number() over w_grp as row_seq_2
    from enriched_subscriptions
    where end_date between '2025-05-04' and '2025-06-24'
    window w_grp as (partition by customer_id order by row_seq desc, id)
),
deduped as (
    select *
    from (
        select *, row_number() over (partition by plan order by monthly_fee_percent_rank desc nulls first, id) as rank_in_group
        from enriched_subscriptions_2
    ) t
    where rank_in_group = 1
)
select
    id,
    status,
    kpi,
    metric_value
from (
    select
        id,
        status,
        monthly_fee_percent_rank,
        monthly_fee
    from deduped
) src
unpivot (
    metric_value for kpi in (monthly_fee_percent_rank, monthly_fee)
) long_rows;
