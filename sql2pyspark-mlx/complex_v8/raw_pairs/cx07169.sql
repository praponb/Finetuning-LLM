WITH prepared_warehouses AS (
    -- Filter warehouses
    SELECT *
    FROM warehouses
    WHERE opened_on >= DATE '2024-02-19'
)
SELECT id AS key_id, opened_on AS event_date
FROM prepared_warehouses
UNION ALL
SELECT id, opened_on
FROM warehouses
UNION ALL
SELECT id, opened_on
FROM warehouses
WHERE city NOT IN ('Singapore', 'Bangkok')
ORDER BY event_date, key_id
