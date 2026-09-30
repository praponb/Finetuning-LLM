WITH prepared AS (
    -- Filter warehouses
    SELECT
        id,
        wh_code,
        capacity,
        opened_on,
        COALESCE(capacity, 0) AS capacity_filled
    FROM warehouses
    WHERE wh_code IS NOT NULL
)
SELECT capacity
FROM prepared
INTERSECT ALL
SELECT id AS capacity
FROM skus
ORDER BY capacity
