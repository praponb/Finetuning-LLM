SELECT DATE_TRUNC('MONTH', launch_date) AS launch_month, brand, MAX(cost) AS max_cost
FROM products
WHERE ARRAY_CONTAINS(tags, 'eco')
GROUP BY 1, 2;
