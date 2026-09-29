SELECT c.country, COUNT(a.id) AS account_count, COUNT(CASE WHEN a.account_type = 'CREDIT' THEN 1 END) AS credit_count
FROM customers c
INNER JOIN accounts a ON a.customer_id = c.id
WHERE a.balance > 37000
GROUP BY c.country
HAVING COUNT(CASE WHEN a.account_type = 'CREDIT' THEN 1 END) >= 2;
