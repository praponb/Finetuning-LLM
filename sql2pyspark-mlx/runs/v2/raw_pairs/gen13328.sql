select product_id, store_id, avg(quantity) as avg_quantity, sum(quantity) as total_quantity from inventory group by product_id, store_id having total_quantity >= 740 order by product_id desc
