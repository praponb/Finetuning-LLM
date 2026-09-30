SELECT id, status
FROM subscriptions
WHERE status NOT IN ('PAUSED', 'ACTIVE', 'EXPIRED')
INTERSECT
SELECT id, status
FROM subscriptions
WHERE end_date <= '2023-06-13';
