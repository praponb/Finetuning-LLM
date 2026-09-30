SELECT QUARTER(last_restock) last_restock_quarter, SUM(quantity) total_quantity, SUM(SUM(quantity)) OVER (ORDER BY QUARTER(last_restock)) AS cumulative_quantity
FROM inventory
WHERE quantity > 200
GROUP BY QUARTER(last_restock)
