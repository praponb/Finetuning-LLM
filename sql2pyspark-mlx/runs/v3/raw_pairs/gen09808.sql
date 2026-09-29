SELECT id, price, stock_qty, CAST(cost AS INT) AS cost_int FROM products WHERE NOT (stock_qty <> 297 AND YEAR(launch_date) = 2024 AND ARRAY_CONTAINS(tags, 'eco')) ORDER BY price ASC
