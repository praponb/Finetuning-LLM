SELECT c.segment, COUNT(*) AS account_count
FROM accounts a
INNER JOIN customers c ON c.id = a.customer_id
WHERE c.email IS NOT NULL
GROUP BY c.segment
HAVING COUNT(*) >= 4
