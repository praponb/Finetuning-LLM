select product_id, min(quantity) as min_quantity from inventory group by product_id;
