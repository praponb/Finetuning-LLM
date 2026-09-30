with product_summary as (
    select product_id, max(last_restock) as latest_last_restock, count(quantity) as quantity_count
    from inventory
    group by product_id
)
select
    products.id,
    products.brand,
    product_summary.latest_last_restock,
    product_summary.quantity_count
from products
inner join product_summary on products.id = product_summary.product_id
order by product_summary.quantity_count, id
limit 20;
