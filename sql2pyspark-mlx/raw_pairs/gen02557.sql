SELECT shipped_date event_date
FROM shipments
WHERE delivered_date BETWEEN '2024-01-01' AND '2024-09-01'
INTERSECT
SELECT opened_date event_date
FROM accounts;
