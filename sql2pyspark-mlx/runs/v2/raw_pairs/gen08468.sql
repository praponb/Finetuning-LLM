SELECT c.id, COUNT(a.id) AS account_count
FROM customers c
LEFT JOIN accounts a ON a.customer_id = c.id
GROUP BY c.id
ORDER BY account_count ASC, c.id DESC;
