SELECT c.segment, c.country, MAX(t.amount) AS max_amount
FROM transactions t
INNER JOIN accounts a ON a.id = t.account_id
INNER JOIN customers c ON a.customer_id = c.id
GROUP BY c.segment, c.country
HAVING MAX(t.amount) >= 1400
ORDER BY country DESC;
