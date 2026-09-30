WITH combined AS (
    -- Stack the branches
    SELECT
        id AS entity_id,
        capacity,
        opened_on AS day,
        'warehouse' AS src
    FROM warehouses
    WHERE UPPER(TRIM(wh_code)) LIKE 'K%'
    UNION ALL
    SELECT
        id,
        capacity,
        opened_on,
        'warehouse_2' AS src
    FROM warehouses
    WHERE YEAR(opened_on) = 2024
      AND capacity > 2614
    UNION ALL
    SELECT
        capacity,
        id,
        opened_on,
        'warehouse_3' AS src
    FROM warehouses
)
SELECT src, COUNT(*) AS n_rows, SUM(capacity) AS total_capacity, MAX(day) AS latest_day
FROM combined
GROUP BY src
ORDER BY src
