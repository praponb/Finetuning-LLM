select id, quantity, unit_price, order_id from order_items where quantity > (select avg(quantity) from order_items) order by quantity
