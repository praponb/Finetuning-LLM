select product_id, total_quantity from (select product_id, sum(quantity) as total_quantity from inventory group by product_id) as sub where total_quantity > 840
