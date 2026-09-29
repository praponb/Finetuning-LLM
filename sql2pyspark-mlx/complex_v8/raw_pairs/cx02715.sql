WITH combined AS (
    -- Stack the branches
    SELECT
        'warehouse' AS src,
        id AS qty,
        city AS label,
        opened_on
    FROM warehouses
    WHERE city <> 'Bangkok'
      AND opened_on BETWEEN DATE '2024-04-13' AND DATE '2025-07-03'
    UNION ALL
    SELECT
        'warehouse_2' AS src,
        capacity,
        wh_code,
        opened_on
    FROM warehouses
)
SELECT src, COUNT(*) AS n_rows, SUM(qty) AS total_qty, MAX(opened_on) AS latest_opened_on
FROM combined
GROUP BY src
ORDER BY src
