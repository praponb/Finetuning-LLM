SELECT product_id, SUM(quantity) AS total_quantity, ROUND(SUM(quantity) * 100.0 / SUM(SUM(quantity)) OVER (), 2) AS pct_of_total
FROM inventory
WHERE YEAR(last_restock) = 2025
GROUP BY product_id;
