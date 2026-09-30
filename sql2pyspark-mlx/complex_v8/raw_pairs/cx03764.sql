SELECT wh_code AS name, opened_on AS event_date, id AS entity_id
FROM warehouses
WHERE city = 'Singapore'
UNION ALL
SELECT wh_code AS name, opened_on AS event_date, id AS entity_id
FROM warehouses
WHERE city = 'Kuala Lumpur'
ORDER BY entity_id DESC, event_date
