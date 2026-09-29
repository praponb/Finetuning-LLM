SELECT QUARTER(last_restock) AS last_restock_quarter, COUNT(quantity) AS quantity_count, ROUND(AVG(quantity), 2) AS avg_quantity FROM inventory GROUP BY QUARTER(last_restock)
