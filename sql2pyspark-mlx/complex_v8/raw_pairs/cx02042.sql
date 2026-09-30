SELECT
    'warehouse' AS source,
    id AS entity_id,
    capacity AS key_id,
    opened_on
FROM warehouses
UNION ALL
SELECT
    'warehouse_2' AS source,
    id,
    capacity,
    opened_on
FROM warehouses
UNION ALL
SELECT
    'warehouse_3' AS source,
    capacity,
    id,
    opened_on
FROM warehouses
