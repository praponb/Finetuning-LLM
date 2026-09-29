SELECT c.segment, COUNT(DISTINCT a.balance) AS unique_account_types, MAX(a.opened_date) AS last_opened_date
FROM accounts a
RIGHT JOIN customers c ON a.customer_id = c.id
WHERE a.customer_id IS NULL
GROUP BY c.segment
ORDER BY 2 DESC, c.segment DESC
LIMIT 5;
