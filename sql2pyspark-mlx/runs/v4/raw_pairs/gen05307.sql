SELECT price FROM products UNION SELECT amount FROM orders WHERE YEAR(order_date) = 2025 ORDER BY price DESC;
