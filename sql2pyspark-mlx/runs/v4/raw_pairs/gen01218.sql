with base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        product_name,
        brand,
        price,
        cost,
        stock_qty,
        supplier_id,
        price * cost as margin,
        price * cost as margin_2
    from products
    where launch_date > date '2023-05-24'
)
-- Window metrics over several different partitions
select
    id,
    product_name,
    brand,
    price,
    cost,
    stock_qty,
    supplier_id,
    margin,
    margin_2,
    lag(stock_qty, 1) over (partition by brand order by margin, id) as prev_stock_qty,
    sum(cost) over (partition by supplier_id) as supplier_id_sum_cost
from base
