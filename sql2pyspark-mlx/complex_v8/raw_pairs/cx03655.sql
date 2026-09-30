SELECT
    'warehouse' AS source,
    id,
    opened_on AS event_date,
    capacity AS entity_id
FROM warehouses
UNION ALL
SELECT
    'warehouse_2' AS source,
    id,
    opened_on,
    capacity
FROM warehouses
