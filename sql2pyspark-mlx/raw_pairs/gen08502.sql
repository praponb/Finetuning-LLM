with filtered as (
    -- Basic cleansing and defaults
    select
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        round(amount, 1) as amount_rounded,
        trim(method) as clean_method,
        coalesce(method, 'OTHER') as method_filled
    from payments
    where paid_at >= to_timestamp('2023-07-17 04:00:00')
      and order_id is null
),
filtered_payments as (
    -- Keep recent, valid rows
    select
        *,
        cast(amount as int) as amount_int,
        coalesce(method, 'NONE') as resolved_method
    from filtered
    where paid_at > date '2023-09-19'
),
remaining_keys as (
    -- Set intersection on keys
    select status
    from filtered_payments
    intersect all
    select status
    from payments
    where order_id in (12, 22, 32, 39)
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from filtered_payments s
    inner join remaining_keys k on s.status = k.status
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        avg(amount_int) over (partition by method order by cast(paid_at as bigint) range between 7776000 preceding and current row) as amount_int_avg_90d,
        min(amount) over (partition by month(paid_at)) as paid_at_month_min_amount,
        dense_rank() over (partition by year(paid_at) order by amount_rounded desc) as amount_rounded_dense_rank,
        max(amount_rounded) over (partition by status order by cast(paid_at as bigint) range between 604800 preceding and current row) as amount_rounded_max_7d
    from reconciled
),
with_dims as (
    -- Attach orders attributes
    select
        a.id,
        a.order_id,
        a.method,
        a.amount_rounded,
        a.amount_int,
        a.resolved_method,
        a.amount_int_avg_90d,
        a.paid_at_month_min_amount,
        a.amount_rounded_max_7d,
        p.status as order_status
    from enriched a
    inner join orders p
        on a.order_id <=> p.id
)
-- Grouped metrics
select
    case when amount_rounded >= 76 then 'PREMIUM' else 'STANDARD' end as amount_rounded_tier,
    order_id,
    case when amount_int_avg_90d >= 68 then 'LARGE' else 'SMALL' end as amount_int_avg_90d_tier,
    count(distinct order_id) as distinct_order_ids,
    sum(case when order_status = 'PENDING' then amount_int else 0 end) as pending_amount_int,
    sum(case when method = 'cash' then amount_rounded_max_7d else 0 end) as cash_amount_rounded_max_7d
from with_dims
group by cube (case when amount_rounded >= 76 then 'PREMIUM' else 'STANDARD' end, order_id, case when amount_int_avg_90d >= 68 then 'LARGE' else 'SMALL' end);
