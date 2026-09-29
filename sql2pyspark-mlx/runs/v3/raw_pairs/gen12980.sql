SELECT DATE_FORMAT(last_restock, 'yyyy-MM') AS last_restock_month, COUNT(*) AS row_count, MAX(quantity) AS max_quantity FROM inventory GROUP BY 1 HAVING row_count >= 4 ORDER BY 1 LIMIT 20;
