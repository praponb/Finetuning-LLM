select oi.id, oi.unit_price, oi.quantity from order_items oi where oi.product_id in (select id from products where category in ('Grocery', 'Home')) order by unit_price, quantity desc
