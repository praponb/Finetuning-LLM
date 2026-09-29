SELECT QUARTER(last_restock) last_restock_quarter, SUM(quantity) total_quantity, MIN(quantity) min_quantity
FROM inventory
GROUP BY last_restock_quarter
ORDER BY last_restock_quarter
