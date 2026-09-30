WITH prepared AS (
    -- Filter warehouses
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        ABS(capacity - 11005) AS capacity_distance
    FROM warehouses
    WHERE opened_on BETWEEN DATE '2024-11-25' AND DATE '2025-07-03'
),
combined AS (
    -- Stack the branches
    SELECT capacity_distance AS key_id, city, opened_on
    FROM prepared
    WHERE UPPER(TRIM(wh_code)) LIKE 'C%'
    UNION ALL
    SELECT capacity, wh_code, opened_on
    FROM warehouses
    UNION ALL
    SELECT id, wh_code, opened_on
    FROM warehouses
)
SELECT city, COUNT(*) AS n_rows, SUM(key_id) AS total_key_id, MAX(opened_on) AS latest_opened_on
FROM combined
GROUP BY city
ORDER BY city
