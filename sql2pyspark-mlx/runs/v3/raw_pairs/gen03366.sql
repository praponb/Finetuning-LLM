with src as (
    select *
    from products
    where category = 'Grocery'
),
Summary as (
    select category, count(stock_qty) as stock_qty_count
    from src
    group by category
)
select *
from Summary
