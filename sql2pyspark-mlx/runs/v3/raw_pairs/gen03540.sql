SELECT MONTH(last_restock) AS last_restock_month, COUNT(*) AS row_count, COUNT(DISTINCT quantity) AS unique_stores FROM inventory GROUP BY MONTH(last_restock)
