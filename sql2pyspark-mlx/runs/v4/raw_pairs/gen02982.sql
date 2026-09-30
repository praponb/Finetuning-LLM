SELECT c.city, ROUND(STDDEV(t.amount), 2) AS stddev_amount FROM transactions t JOIN accounts a ON a.id = t.account_id JOIN customers c ON c.id = a.customer_id GROUP BY c.city ORDER BY stddev_amount;
