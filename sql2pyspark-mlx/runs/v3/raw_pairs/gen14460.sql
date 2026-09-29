SELECT c.city, COUNT(a.id) account_count FROM customers c LEFT OUTER JOIN accounts a ON a.customer_id = c.id GROUP BY c.city HAVING COUNT(a.id) > 4 ORDER BY c.city, account_count LIMIT 3
