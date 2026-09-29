with valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        order_id,
        quantity,
        unit_price,
        unit_price + quantity as line_total
    from order_items
    where product_id is not null
)
-- Running and moving aggregates
select
    id,
    order_id,
    quantity,
    unit_price,
    line_total,
    first_value(line_total, true) over (partition by order_id order by line_total, id rows between unbounded preceding and current row) as first_known_line_total,
    lag(quantity, 1) over (partition by order_id order by quantity, id) as prev_quantity,
    dense_rank() over (partition by order_id order by unit_price desc) as unit_price_dense_rank,
    sum(unit_price) over (partition by order_id order by quantity, id rows between unbounded preceding and current row) as running_unit_price,
    dense_rank() over (partition by order_id order by quantity) as quantity_dense_rank
from valid
where line_total is not null
