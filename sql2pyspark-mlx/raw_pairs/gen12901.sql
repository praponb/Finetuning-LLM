SELECT customer_id, COUNT(CASE WHEN status = 'FROZEN' THEN 1 END) AS frozen_count, MAX(opened_date) AS last_opened_date
FROM accounts
WHERE customer_id IS NULL
GROUP BY customer_id
HAVING COUNT(CASE WHEN status = 'FROZEN' THEN 1 END) < 4
ORDER BY customer_id DESC;
