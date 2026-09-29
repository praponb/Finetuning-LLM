SELECT DATE_FORMAT(last_restock, 'yyyy-MM') AS last_restock_month, SUM(quantity) AS total_quantity, DENSE_RANK() OVER (ORDER BY SUM(quantity) DESC) AS quantity_rank
FROM inventory
WHERE last_restock BETWEEN '2023-05-04' AND '2023-08-01'
GROUP BY DATE_FORMAT(last_restock, 'yyyy-MM')
