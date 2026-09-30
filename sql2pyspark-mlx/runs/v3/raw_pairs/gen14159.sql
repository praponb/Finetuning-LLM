SELECT supplier_id, ROUND(cost * 1.1, 2) AS adjusted_cost FROM products WHERE category = 'Home' ORDER BY supplier_id ASC;
