SELECT start_date
FROM subscriptions
WHERE status IN ('CANCELLED', 'EXPIRED')
INTERSECT
SELECT opened_date AS start_date
FROM accounts;
