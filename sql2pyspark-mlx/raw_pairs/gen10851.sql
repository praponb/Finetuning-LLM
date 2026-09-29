SELECT c.country, ROUND(AVG(a.balance), 2) AS avg_balance, ROUND((100 * SUM(CASE WHEN a.status = 'FROZEN' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_frozen
FROM accounts a
LEFT OUTER JOIN customers c ON a.customer_id = c.id
WHERE a.balance > 45000
GROUP BY c.country
ORDER BY avg_balance
