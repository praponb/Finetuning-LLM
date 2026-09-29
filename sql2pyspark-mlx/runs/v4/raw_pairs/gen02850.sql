with order_items_by_order as (
    select order_id, count(distinct quantity) unique_products
    from order_items
    where quantity = 4
    group by order_id
)
select
    o.id,
    o.shipping_fee,
    o.status,
    oibo.unique_products
from orders o
left join order_items_by_order oibo on o.id = oibo.order_id
where oibo.unique_products > 9
order by o.shipping_fee;
