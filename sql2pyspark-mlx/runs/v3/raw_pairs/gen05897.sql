select product_id, count(quantity) as quantity_count, avg(quantity) as avg_quantity from order_items where order_id in (6, 11) group by product_id;
