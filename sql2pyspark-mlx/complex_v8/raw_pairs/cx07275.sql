WITH unioned AS (
    -- Stack the branches
    SELECT move_type, operator AS category, unit_price AS metric
    FROM stock_moves
    UNION ALL
    SELECT move_type, operator, unit_price
    FROM stock_moves
    WHERE qty >= 109
)
SELECT move_type, COUNT(*) AS n_rows, SUM(metric) AS total_metric
FROM unioned
GROUP BY move_type
ORDER BY move_type
