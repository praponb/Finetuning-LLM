SELECT YEAR(store_sales.sale_date) sale_year, store_sales.product_id, MAX(inventory.last_restock) last_last_restock
FROM inventory
LEFT OUTER JOIN store_sales ON inventory.store_id = store_sales.store_id AND inventory.product_id = store_sales.product_id
GROUP BY YEAR(store_sales.sale_date), store_sales.product_id
