select i.last_restock, ss.store_id, ss.product_id, ss.revenue from inventory i left join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id where i.store_id = 4
