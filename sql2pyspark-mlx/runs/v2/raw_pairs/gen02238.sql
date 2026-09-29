SELECT *, CAST(cost AS INT) AS cost_int FROM products WHERE ARRAY_CONTAINS(tags, 'premium') AND supplier_id = 9 AND YEAR(launch_date) = 2023 ORDER BY cost_int, stock_qty, id LIMIT 1
