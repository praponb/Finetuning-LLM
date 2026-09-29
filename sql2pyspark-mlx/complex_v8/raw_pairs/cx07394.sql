WITH recent AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        moved_at + INTERVAL 2 HOURS AS moved_at_plus_2h,
        QUARTER(moved_at) AS moved_at_quarter
    FROM stock_moves
),
scored AS (
    -- Derive helper columns
    SELECT *, LOWER(TRIM(move_type)) AS move_type_clean
    FROM recent
),
unique_rows AS (
    -- Keep the earliest row per move_type
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        moved_at_plus_2h,
        moved_at_quarter,
        move_type_clean
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY move_type ORDER BY moved_at_plus_2h, id) AS rn
        FROM scored
    ) ranked
    WHERE rn = 1
),
per_group AS (
    -- Summarise per group
    SELECT
        YEAR(moved_at_plus_2h) AS moved_at_plus_2h_year,
        move_type,
        PERCENTILE_APPROX(moved_at_quarter, 0.5) AS median_moved_at_quarter,
        COUNT(*) AS cnt,
        ROUND(STDDEV(qty), 2) AS stddev_qty,
        SUM(CASE WHEN operator IS NOT NULL THEN 1 ELSE 0 END) AS n_operator_is_not_null,
        COUNT(DISTINCT move_type) AS distinct_move_types
    FROM unique_rows
    GROUP BY YEAR(moved_at_plus_2h), move_type
    HAVING MAX(qty) > 137
)
SELECT moved_at_plus_2h_year, move_type, field, value
FROM (
    SELECT moved_at_plus_2h_year, move_type, distinct_move_types, n_operator_is_not_null, median_moved_at_quarter
    FROM per_group
) src
UNPIVOT INCLUDE NULLS (
    value FOR field IN (distinct_move_types AS m_distinct_move_types, n_operator_is_not_null AS n_operator_is_not_null_v, median_moved_at_quarter AS m_median_moved_at_quarter)
) unp
ORDER BY moved_at_plus_2h_year, move_type, field
