select
    p.id,
    p.category,
    count(oi.id) as order_item_count,
    count(distinct oi.quantity) as unique_products,
    count(oi.unit_price) as unit_price_count
from products p
left join order_items oi on p.id = oi.product_id
where oi.unit_price <= 340
group by p.id, p.category;
