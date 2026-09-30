select p.brand, count(oi.id) as order_item_count, sum(oi.unit_price) as total_unit_price
from products p
left join order_items oi on p.id = oi.product_id
where oi.order_id in (3, 37)
group by p.brand
order by total_unit_price
