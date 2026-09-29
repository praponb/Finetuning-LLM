SELECT delivered_date
FROM shipments
UNION
SELECT opened_date
FROM accounts
WHERE status = 'CLOSED'
ORDER BY delivered_date
