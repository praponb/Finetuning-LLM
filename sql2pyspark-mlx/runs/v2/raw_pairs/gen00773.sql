SELECT id, supplier_id, cost, ROUND(cost / SUM(cost) OVER (PARTITION BY supplier_id), 3) AS cost_share FROM products
