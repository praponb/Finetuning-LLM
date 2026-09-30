WITH combined AS (
    -- Stack the branches
    SELECT
        qty,
        id,
        move_type,
        'move' AS src
    FROM stock_moves
    UNION ALL
    SELECT
        id,
        capacity,
        wh_code,
        'warehouse' AS src
    FROM warehouses
    WHERE capacity < 11506
)
SELECT src, COUNT(*) AS n_rows, SUM(qty) AS total_qty
FROM combined
GROUP BY src
ORDER BY src
