select product_id, max(quantity) as max_quantity from inventory group by 1;
