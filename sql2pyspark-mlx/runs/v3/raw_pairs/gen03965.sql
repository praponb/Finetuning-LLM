SELECT last_restock, store_id, product_id, DATE_TRUNC('year', last_restock) AS last_restock_period FROM inventory WHERE last_restock >= '2023-07-01' ORDER BY product_id DESC
