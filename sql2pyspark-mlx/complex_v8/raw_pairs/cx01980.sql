WITH prepared AS (
    -- Scope the warehouses rows
    SELECT
        id,
        wh_code,
        capacity,
        opened_on,
        DATEDIFF(DATE '2025-06-30', opened_on) AS days_since_opened_on
    FROM warehouses
    WHERE city = 'Bangkok'
)
SELECT capacity
FROM prepared
INTERSECT ALL
SELECT id AS capacity
FROM warehouses
ORDER BY capacity
