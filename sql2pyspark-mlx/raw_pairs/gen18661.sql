select product_id, round(avg(unit_price), 2) as avg_unit_price from order_items where quantity is not null or product_id = 40 group by product_id order by avg_unit_price desc
