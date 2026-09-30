SELECT c.segment, COUNT(a.id) AS account_count
FROM customers c
LEFT JOIN accounts a ON c.id = a.customer_id
WHERE c.signup_date >= '2024-03-01'
GROUP BY c.segment
ORDER BY account_count DESC, segment
LIMIT 3
