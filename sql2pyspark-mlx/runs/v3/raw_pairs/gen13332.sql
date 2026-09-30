select product_id, max(quantity) as max_quantity, count(quantity) as quantity_count from inventory where quantity <= 212 group by product_id
