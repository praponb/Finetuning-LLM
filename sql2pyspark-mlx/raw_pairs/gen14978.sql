with metrics as (
    select
        id,
        order_id,
        amount,
        paid_at,
        status,
        max(amount) over (partition by status order by amount, id rows between 2 preceding and current row) as last3_max_amount,
        count(amount) over (partition by status order by amount, id rows between 2 preceding and current row) as last3_count_amount,
        sum(amount) over (partition by status order by amount, id rows between 2 preceding and current row) as last3_sum_amount,
        ntile(3) over (partition by order_id order by amount, id) as amount_bucket
    from payments
),
kept_keys as (
    select status, order_id
    from metrics
    intersect
    select status, order_id
    from payments
    where order_id = 20
),
reconciled as (
    select s.*
    from metrics s
    inner join kept_keys k on s.status = k.status and s.order_id = k.order_id
),
metrics_payments as (
    select
        id,
        order_id,
        amount,
        paid_at,
        max(amount_bucket) over (partition by month(paid_at) order by cast(paid_at as bigint) range between 7776000 preceding and current row) as amount_bucket_max_90d,
        count(amount) over (partition by order_id, status order by cast(paid_at as bigint) range between 2592000 preceding and current row) as amount_count_30d,
        rank() over (partition by status, order_id order by amount nulls first) as amount_rank
    from reconciled
),
joined as (
    select a.*, o.shipping_fee
    from metrics_payments a
    inner join orders o
        on a.order_id = o.id
    where o.shipping_fee < 29
),
metrics_payments_2 as (
    select
        order_id,
        amount,
        amount_bucket_max_90d,
        amount_count_30d,
        percent_rank() over (partition by order_id order by amount_bucket_max_90d nulls first) as amount_bucket_max_90d_percent_rank,
        max(amount_count_30d) over (partition by order_id) as order_id_max_amount_count_30d
    from joined
)
select
    case when amount_bucket_max_90d >= 8 then 'LARGE' else 'SMALL' end as amount_bucket_max_90d_tier,
    case when amount >= 1350 then 'PREMIUM' else 'STANDARD' end as amount_tier,
    approx_percentile(amount_bucket_max_90d_percent_rank, 0.5) as amount_bucket_max_90d_percent_rank_p50,
    percentile_approx(amount, array(0.25, 0.5, 0.75)) as amount_quartiles,
    percentile_approx(amount, 0.5) as amount_p50,
    max(amount_bucket_max_90d_percent_rank) as max_amount_bucket_max_90d_percent_rank
from metrics_payments_2
group by case when amount_bucket_max_90d >= 8 then 'LARGE' else 'SMALL' end, case when amount >= 1350 then 'PREMIUM' else 'STANDARD' end
order by amount_p50 desc;
