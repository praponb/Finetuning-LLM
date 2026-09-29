SELECT id, opened_on AS event_date
FROM warehouses
UNION ALL
SELECT capacity AS id, opened_on AS event_date
FROM warehouses
UNION ALL
SELECT id, opened_on AS event_date
FROM warehouses
WHERE opened_on >= DATE '2024-04-13'
ORDER BY id, event_date
