with valid as (
    -- Keep recent, valid rows
    select
        id,
        category,
        brand,
        price,
        stock_qty,
        launch_date,
        concat(category, ' - ', brand) as category_brand
    from products
    where launch_date > to_date('2023-08-10')
      and supplier_id is not null
)
-- Window metrics over several different partitions
select
    id,
    category,
    brand,
    price,
    stock_qty,
    launch_date,
    category_brand,
    sum(price) over (partition by category, brand order by price asc, id asc rows between unbounded preceding and current row) as running_price,
    percent_rank() over (partition by brand order by stock_qty desc nulls last) as stock_qty_percent_rank,
    percent_rank() over (order by price asc nulls last) as price_percent_rank
from valid
