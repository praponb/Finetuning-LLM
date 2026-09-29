SELECT QUARTER(last_restock) last_restock_quarter, SUM(quantity) total_quantity, DENSE_RANK() OVER (ORDER BY SUM(quantity) DESC) AS quantity_rank
FROM inventory
GROUP BY QUARTER(last_restock)
ORDER BY total_quantity DESC;
