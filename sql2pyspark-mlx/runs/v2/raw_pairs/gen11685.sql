SELECT c.city, SUM(CASE WHEN a.status = 'FROZEN' THEN 1 ELSE 0 END) AS frozen_count, MAX(a.balance) - MIN(a.balance) AS balance_range
FROM accounts a
FULL OUTER JOIN customers c ON c.id = a.customer_id
GROUP BY c.city
