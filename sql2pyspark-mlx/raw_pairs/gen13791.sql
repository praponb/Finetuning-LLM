with flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(paid_at) over (partition by method order by paid_at, id) is null
                 or paid_at > lag(paid_at) over (partition by method order by paid_at, id) + interval 14 days then 1 else 0 end as is_new_session
    from payments
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by method order by paid_at, id rows between unbounded preceding and current row) as burst_id
    from flagged
),
enriched as (
    -- Attach orders attributes
    select
        e.id,
        e.order_id,
        e.amount,
        e.paid_at,
        e.status,
        e.is_new_session,
        o.discount,
        o.order_date
    from islands e
    left join orders o
        on e.order_id = o.id
),
filtered as (
    -- Keep recent, valid rows
    select
        *,
        cast(paid_at as date) as paid_at_day,
        ceil(discount) as discount_whole
    from enriched
    where status in ('FAILED', 'SUCCESS', 'REFUNDED')
),
enriched_payments as (
    -- Running and moving aggregates
    select
        id,
        paid_at,
        status,
        is_new_session,
        discount,
        order_date,
        paid_at_day,
        discount_whole,
        sum(discount_whole) over (partition by order_id, status order by cast(paid_at as bigint) range between 7776000 preceding and current row) as discount_whole_sum_90d,
        max(discount_whole) over (partition by status order by cast(paid_at as bigint) range between 7776000 preceding and current row) as discount_whole_max_90d,
        sum(discount_whole) over (partition by order_id order by cast(paid_at as bigint) range between 7776000 preceding and current row) as discount_whole_sum_90d_2,
        dense_rank() over (order by discount nulls first) as discount_dense_rank
    from filtered
    where status in ('FAILED', 'REFUNDED')
),
deduped as (
    -- Keep one row per key (latest first)
    select *
    from enriched_payments
    qualify row_number() over (partition by status order by order_date nulls first, id) = 1
)
-- Multi-level aggregation
select
    status,
    sum(discount_whole_max_90d) filter (where status = 'REFUNDED') as refunded_total_discount_whole_max_90d,
    count(case when status = 'SUCCESS' then 1 end) as success_count,
    percentile_approx(discount_whole_max_90d, 0.95) as discount_whole_max_90d_p95,
    count(*) filter (where status = 'SUCCESS') as success_rows,
    min(paid_at) as first_paid_at,
    percentile_approx(discount_whole_sum_90d_2, array(0.25, 0.5, 0.75)) as discount_whole_sum_90d_2_quartiles
from deduped
group by status
order by success_rows desc nulls first;
