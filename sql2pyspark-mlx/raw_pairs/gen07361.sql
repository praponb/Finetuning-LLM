with base as (
    select
        id,
        order_id,
        method,
        amount,
        status,
        cast(amount as int) as amount_int,
        date_format(paid_at, 'MMM yyyy') as paid_at_label,
        coalesce(method, 'DIRECT') as method_clean
    from payments
    where order_id is not null
),
enriched as (
    select
        src.id,
        src.order_id,
        src.method,
        src.amount,
        src.paid_at_label,
        src.method_clean,
        b.order_date,
        b.amount as order_amount
    from base src
    left join orders b
        on src.order_id = b.id
),
deduped as (
    select *
    from (
        select *, row_number() over (partition by order_id order by order_date desc nulls first, id) as latest_rank
        from enriched
    ) ranked
    where latest_rank = 1
),
metrics as (
    select
        id,
        order_id,
        method,
        amount,
        paid_at_label,
        method_clean,
        latest_rank,
        ntile(4) over (partition by method, order_id order by amount desc, id) as amount_bucket,
        lead(order_amount, 2) over (partition by method order by order_amount, id) as next_order_amount
    from deduped
    where method != 'cash'
),
with_windows as (
    select
        id,
        paid_at_label,
        method_clean,
        latest_rank,
        amount_bucket,
        next_order_amount,
        cume_dist() over (order by next_order_amount nulls first) as next_order_amount_cume_dist,
        sum(amount_bucket) over (order by amount_bucket, id rows between unbounded preceding and current row) as running_amount_bucket,
        stddev_samp(next_order_amount) over (partition by method) as method_stddev_samp_next_order_amount,
        round(amount / sum(amount) over (partition by method), 4) as amount_share
    from metrics
),
filtered as (
    select
        id,
        paid_at_label,
        latest_rank,
        amount_bucket,
        next_order_amount,
        next_order_amount_cume_dist,
        running_amount_bucket,
        method_stddev_samp_next_order_amount,
        next_order_amount_cume_dist - next_order_amount as next_order_amount_cume_dist_minus_next_order_amount,
        case when running_amount_bucket >= 40 then 'High' when running_amount_bucket >= 16 then 'Medium' else 'Low' end as running_amount_bucket_band
    from with_windows
    where next_order_amount_cume_dist between 0.41 and 0.64
),
enriched_payments as (
    select
        id,
        paid_at_label,
        latest_rank,
        next_order_amount,
        method_stddev_samp_next_order_amount,
        next_order_amount_cume_dist_minus_next_order_amount,
        lag(next_order_amount_cume_dist_minus_next_order_amount) over (order by next_order_amount_cume_dist, id) as prev_next_order_amount_cume_dist_minus_next_order_amount,
        lead(amount_bucket) over (order by next_order_amount_cume_dist, id) as next_amount_bucket,
        row_number() over (order by next_order_amount_cume_dist, id) as seq_no
    from filtered
)
select
    id,
    paid_at_label,
    next_order_amount,
    method_stddev_samp_next_order_amount,
    next_amount_bucket,
    seq_no
from enriched_payments;
