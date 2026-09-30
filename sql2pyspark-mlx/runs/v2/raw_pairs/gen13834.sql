SELECT id, price, cost FROM products WHERE price = (SELECT MIN(price) FROM products) ORDER BY cost, price
