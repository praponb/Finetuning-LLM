SELECT category, brand, ROUND(STDDEV(cost), 2) stddev_cost FROM products WHERE supplier_id IS NOT NULL AND price <= 580 GROUP BY category, brand ORDER BY category DESC, brand LIMIT 5;
