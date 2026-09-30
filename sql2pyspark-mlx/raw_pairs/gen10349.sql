with latest as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by product_id order by quantity, id) as rn
        from order_items
    ) t
    where rn = 1
),
latest_order_items as (
    -- QUALIFY-style window filter
    select
        id,
        order_id,
        quantity
    from latest
    qualify row_number() over (partition by order_id order by unit_price desc nulls first, id) = 1
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        order_id,
        sum(quantity) over (partition by order_id order by quantity, id rows between unbounded preceding and current row) as running_quantity,
        ntile(3) over (order by quantity desc, id) as quantity_bucket
    from latest_order_items
    where order_id in (12, 31, 37)
),
cleaned as (
    -- Keep recent, valid rows
    select *, round(quantity_bucket, 2) as quantity_bucket_rounded
    from metrics
    where order_id in (17, 18)
),
remaining_keys as (
    -- Set intersection on keys
    select id
    from cleaned
    intersect all
    select id
    from order_items
    where quantity >= 3
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from cleaned s
    inner join remaining_keys k on s.id = k.id
)
-- Join a dimension and aggregate
select
    d.status,
    count(*) as num_rows,
    min(a.quantity_bucket_rounded) as min_quantity_bucket_rounded
from reconciled a
left join orders d on a.order_id = d.id
group by d.status
