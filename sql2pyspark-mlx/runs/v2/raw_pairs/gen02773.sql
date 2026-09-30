SELECT id, cost FROM products WHERE cost <= (SELECT MIN(cost) FROM products) ORDER BY cost DESC
