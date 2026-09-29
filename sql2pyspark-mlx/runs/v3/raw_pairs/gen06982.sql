SELECT id, cost, product_name
FROM products
WHERE cost > (SELECT MAX(cost) FROM products WHERE price < 650)
