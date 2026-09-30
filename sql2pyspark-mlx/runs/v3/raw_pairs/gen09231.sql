SELECT start_date
FROM subscriptions
WHERE status IN ('CANCELLED', 'PAUSED', 'ACTIVE')
INTERSECT
SELECT opened_date AS start_date
FROM accounts
WHERE balance BETWEEN 62000 AND 67000
ORDER BY start_date DESC;
