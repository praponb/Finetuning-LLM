SELECT cost, ROUND(cost * 1.1, 2) AS adjusted_cost FROM products WHERE category = 'Fashion' AND category <> 'Grocery' ORDER BY adjusted_cost DESC, cost DESC;
