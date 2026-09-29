SELECT status
FROM accounts
WHERE status != 'CLOSED'
INTERSECT
SELECT status
FROM subscriptions
WHERE end_date BETWEEN '2024-08-01' AND '2025-10-17'
