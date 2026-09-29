SELECT id, price
FROM products
WHERE price <= (SELECT MIN(price) FROM products)
ORDER BY id
