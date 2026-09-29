select order_id, product_id, max(unit_price) - min(unit_price) as unit_price_range
from order_items
where unit_price > 200 and order_id in (16, 17, 37, 38)
group by order_id, product_id
order by unit_price_range desc, order_id, product_id
limit 3
