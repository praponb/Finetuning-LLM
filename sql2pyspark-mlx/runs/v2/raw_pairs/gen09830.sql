SELECT supplier_id, SUM(cost) AS total_cost, SUM(SUM(cost)) OVER (ORDER BY supplier_id) AS cumulative_cost
FROM products
WHERE supplier_id IS NOT NULL
GROUP BY supplier_id
ORDER BY total_cost;
