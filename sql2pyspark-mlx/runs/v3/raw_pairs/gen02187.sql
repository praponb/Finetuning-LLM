SELECT amount FROM orders INTERSECT SELECT price AS amount FROM products ORDER BY amount DESC
