with order_items_by_product as (
    select product_id, sum(quantity) as total_quantity
    from order_items
    where product_id = 25
    group by product_id
)
select
    p.id,
    p.price,
    p.launch_date,
    oibp.total_quantity
from products p
inner join order_items_by_product oibp on p.id = oibp.product_id
where oibp.total_quantity > 31;
