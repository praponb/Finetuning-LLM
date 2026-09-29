SELECT QUARTER(p.launch_date) AS launch_quarter, MIN(i.last_restock) AS first_last_restock
FROM inventory i
RIGHT JOIN products p ON i.product_id = p.id
GROUP BY QUARTER(p.launch_date)
ORDER BY first_last_restock DESC;
