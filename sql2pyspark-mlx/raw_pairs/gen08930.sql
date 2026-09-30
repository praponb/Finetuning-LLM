select id, unit_price, quantity, lag(quantity, 2) over (order by unit_price, id) as prev_quantity from order_items where product_id is not null order by quantity desc;
