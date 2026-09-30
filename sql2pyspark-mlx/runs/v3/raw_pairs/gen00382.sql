with product_stats as (
    select product_id, avg(quantity) as avg_quantity, count(distinct quantity) as unique_products
    from inventory
    where product_id in (6, 7, 8, 10)
    group by product_id
)
select
    products.id,
    products.product_name,
    products.launch_date,
    product_stats.avg_quantity,
    product_stats.unique_products
from products
left join product_stats on products.id = product_stats.product_id
