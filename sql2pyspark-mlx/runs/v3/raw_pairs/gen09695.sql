SELECT product_id, DATE_TRUNC('MONTH', last_restock) AS last_restock_month, COUNT(quantity) AS quantity_count
FROM inventory
GROUP BY product_id, last_restock_month
