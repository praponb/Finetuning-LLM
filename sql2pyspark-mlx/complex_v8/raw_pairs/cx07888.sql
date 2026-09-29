WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM warehouses
    WHERE opened_on IS NOT NULL
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_TRUNC('MONTH', opened_on) AS opened_on_month,
        LOWER(REGEXP_REPLACE(TRIM(city), ' ', '_')) AS city_slug
    FROM filtered
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT warehouse_id, MIN(unit_price) AS min_move_unit_price
    FROM stock_moves
    GROUP BY warehouse_id
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.min_move_unit_price
    FROM annotated s
    LEFT JOIN move_stats st ON st.warehouse_id = s.id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(min_move_unit_price / NULLIF(SUM(min_move_unit_price) OVER (PARTITION BY city), 0), 4) AS min_move_unit_price_share_of_city,
        DENSE_RANK() OVER (ORDER BY capacity DESC) AS capacity_dense_rank,
        ROUND(CUME_DIST() OVER w_all_by_capacity_ties, 3) AS capacity_cume_dist
    FROM summarized_rows
    WINDOW w_all_by_capacity_ties AS (ORDER BY capacity DESC)
),
move_stats_2 AS (
    -- Per-warehouse stock_moves metrics
    SELECT
        warehouse_id,
        SUM(qty) AS total_move_qty,
        MAX(qty) AS max_move_qty,
        MIN(moved_at) AS first_moved_at
    FROM stock_moves
    GROUP BY warehouse_id
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.total_move_qty,
        st.max_move_qty,
        st.first_moved_at
    FROM metrics s
    LEFT JOIN move_stats_2 st ON st.warehouse_id = s.id
),
summary AS (
    -- Group-level totals
    SELECT
        city,
        DATE_FORMAT(opened_on_month, 'yyyy-MM') AS opened_on_month_ym,
        COUNT(*) FILTER (WHERE city <> 'Singapore') AS n_city_singapore,
        COUNT(DISTINCT city) AS distinct_citys,
        SUM(CASE WHEN city IN ('Singapore', 'Kuala Lumpur') THEN 1 ELSE 0 END) AS n_city_in_singapore_kuala_lump,
        MAX(opened_on_month) AS last_opened_on_month
    FROM with_counts
    GROUP BY 1, 2
    HAVING SUM(min_move_unit_price_share_of_city) > 0.2
)
-- Final projection
SELECT
    city,
    opened_on_month_ym,
    distinct_citys,
    n_city_in_singapore_kuala_lump
FROM summary
ORDER BY city DESC
