SELECT c.segment, ROUND(AVG(a.balance), 2) AS avg_balance
FROM accounts a
INNER JOIN customers c ON a.customer_id = c.id
WHERE c.city NOT IN ('London', 'Chiang Mai', 'Berlin')
GROUP BY c.segment
