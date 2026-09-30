SELECT plan, start_date
FROM subscriptions
WHERE status IN ('ACTIVE', 'EXPIRED', 'CANCELLED')
UNION ALL
SELECT plan, start_date
FROM subscriptions
WHERE start_date >= '2023-10-06'
ORDER BY start_date;
