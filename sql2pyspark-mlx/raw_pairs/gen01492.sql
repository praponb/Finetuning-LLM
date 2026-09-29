SELECT opened_date AS event_date
FROM accounts
UNION ALL
SELECT delivered_date AS event_date
FROM shipments
ORDER BY event_date
