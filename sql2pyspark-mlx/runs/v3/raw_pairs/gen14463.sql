SELECT MONTH(last_restock) AS last_restock_month, product_id, COUNT(*) AS row_count
FROM inventory
GROUP BY MONTH(last_restock), product_id
HAVING COUNT(quantity) > 3
