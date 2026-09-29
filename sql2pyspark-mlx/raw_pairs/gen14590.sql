SELECT * FROM inventory QUALIFY RANK() OVER (PARTITION BY product_id ORDER BY last_restock DESC) = 1
