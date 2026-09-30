with valid as (
    select
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        unit_price - quantity as line_total
    from order_items
    where product_id is not null
),
latest as (
    select *
    from (
        select *, row_number() over (partition by order_id order by line_total desc, id) as dedup_rank
        from valid
    ) ranked
    where dedup_rank = 1
),
enriched as (
    select
        id,
        product_id,
        unit_price,
        line_total,
        dedup_rank,
        sum(quantity) over (partition by product_id, order_id order by quantity, id rows between unbounded preceding and current row) as running_quantity,
        lead(unit_price, 2) over (partition by product_id order by dedup_rank, id) as next_unit_price
    from latest
    where line_total > 50
),
remaining_keys as (
    select id, product_id
    from enriched
    intersect all
    select id, product_id
    from order_items
    where order_id in (5, 16)
),
kept_rows as (
    select s.*
    from enriched s
    inner join remaining_keys k on s.id = k.id and s.product_id = k.product_id
)
select
    product_id,
    metric,
    reading
from (
    select
        product_id,
        next_unit_price,
        line_total,
        unit_price
    from kept_rows
) src
unpivot include nulls (
    reading for metric in (next_unit_price, line_total, unit_price)
)
