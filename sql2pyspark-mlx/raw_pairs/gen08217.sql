with valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        case when status in ('FAILED', 'SUCCESS') then 'Tier 1' else 'Other' end as status_group,
        ceil(amount) as amount_whole,
        coalesce(method, 'OTHER') as method_filled
    from payments
    where amount between 1420 and 1440
      and method is not null
),
enriched as (
    -- Attach orders attributes
    select src.*, b.order_date
    from valid src
    left join orders b
        on src.order_id = b.id
),
joined as (
    -- Attach orders attributes
    select
        a.id,
        a.order_id,
        a.method,
        a.paid_at,
        a.status,
        a.method_filled,
        a.order_date,
        b.status as order_status
    from enriched a
    left join orders b
        on a.order_id = b.id
    where b.store_id in (3, 19, 35)
),
enriched_payments as (
    -- Attach orders attributes
    select
        src.id,
        src.order_id,
        src.paid_at,
        src.method_filled,
        src.order_date,
        src.order_status,
        d.discount,
        d.order_date as order_order_date
    from joined src
    left join orders d
        on src.order_id <=> d.id
),
scored as (
    -- Running and moving aggregates
    select
        id,
        order_id,
        paid_at,
        order_status,
        count(discount) over (partition by order_status order by order_date, id rows between unbounded preceding and current row) as running_count_discount,
        sum(discount) over (partition by order_status order by order_date, id rows between unbounded preceding and current row) as running_sum_discount,
        max(discount) over (partition by order_status order by order_date, id rows between unbounded preceding and current row) as running_max_discount,
        count(discount) over (order by cast(paid_at as bigint) range between 7776000 preceding and current row) as discount_count_90d
    from enriched_payments
),
scored_payments as (
    -- Window metrics over several different partitions
    select
        *,
        count(discount_count_90d) over (partition by order_id, order_status order by running_count_discount, id rows between 6 preceding and current row) as last7_count_discount_count_90d,
        avg(discount_count_90d) over (partition by order_id, order_status order by running_count_discount, id rows between 6 preceding and current row) as last7_avg_discount_count_90d,
        max(discount_count_90d) over (partition by order_id, order_status order by running_count_discount, id rows between 6 preceding and current row) as last7_max_discount_count_90d,
        avg(discount_count_90d) over (partition by order_status, order_id order by discount_count_90d, id rows between 6 preceding and 1 preceding) as discount_count_90d_avg_6_prior
    from scored
),
latest as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by order_status, order_id order by last7_count_discount_count_90d nulls first, id) as dedup_rank
    from scored_payments
    qualify dedup_rank = 1
)
-- Native SQL UNPIVOT (wide to long)
select
    id,
    order_status,
    metric_name,
    val
from (
    select
        id,
        order_status,
        running_max_discount,
        last7_avg_discount_count_90d,
        running_sum_discount
    from latest
) src
unpivot (
    val for metric_name in (running_max_discount as RUNNING_MAX_DISCOUNT_VAL, last7_avg_discount_count_90d, running_sum_discount)
);
