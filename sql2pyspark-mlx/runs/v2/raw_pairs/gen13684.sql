SELECT id, cost FROM products WHERE cost >= (SELECT MAX(cost) FROM products) ORDER BY id;
