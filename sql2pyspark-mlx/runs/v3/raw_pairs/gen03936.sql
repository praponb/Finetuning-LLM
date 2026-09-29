with top_products as (
    select *, row_number() over (partition by category order by stock_qty, id) as row_num
    from products
)
select
    id,
    category,
    stock_qty,
    brand
from top_products
where row_num <= 1
order by brand desc;
