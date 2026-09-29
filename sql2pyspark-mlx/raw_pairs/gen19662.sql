select id, quantity, product_id, unit_price from order_items where quantity > (select avg(quantity) from order_items where product_id in (31, 32, 34, 39))
