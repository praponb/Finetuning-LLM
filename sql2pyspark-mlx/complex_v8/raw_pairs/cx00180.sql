WITH stacked AS (
    -- Stack the branches
    SELECT
        'warehouse' AS record_type,
        id AS qty,
        capacity,
        opened_on
    FROM warehouses
    UNION ALL
    SELECT
        'warehouse_2' AS record_type,
        id,
        capacity,
        opened_on
    FROM warehouses
    WHERE capacity >= 4912
)
SELECT record_type, COUNT(*) AS n_rows, SUM(qty) AS total_qty, MAX(opened_on) AS latest_opened_on
FROM stacked
GROUP BY record_type
ORDER BY record_type
