SELECT capacity, id, opened_on AS event_date
FROM warehouses
WHERE capacity <= 11506
UNION ALL
SELECT capacity, id, opened_on AS event_date
FROM warehouses
