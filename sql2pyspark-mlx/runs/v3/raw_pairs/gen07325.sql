select product_id, month(last_restock) as last_restock_month, min(quantity) as min_quantity, avg(quantity) as avg_quantity from inventory group by 1, 2
