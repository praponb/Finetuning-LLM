SELECT MONTH(p.launch_date) AS launch_month, AVG(i.quantity) AS avg_quantity
FROM inventory i
INNER JOIN products p ON p.id = i.product_id
GROUP BY MONTH(p.launch_date)
HAVING avg_quantity >= 80
ORDER BY launch_month DESC;
