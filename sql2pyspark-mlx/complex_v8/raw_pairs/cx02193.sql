WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        FLOOR(capacity / 5000) * 5000 AS capacity_bucket,
        UPPER(TRIM(wh_code)) AS wh_code_clean,
        MONTH(opened_on) AS opened_on_month
    FROM warehouses
    WHERE opened_on >= DATE '2024-11-25'
),
deduped AS (
    -- Keep the latest row per city
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        capacity_bucket,
        wh_code_clean,
        opened_on_month
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY city ORDER BY capacity_bucket DESC, id DESC) AS recency_rank
        FROM scoped
    ) ranked
    WHERE recency_rank = 1
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT
        warehouse_id,
        COUNT(*) FILTER (WHERE move_type NOT IN ('INBOUND', 'OUTBOUND')) AS move_move_type_count
    FROM stock_moves
    GROUP BY warehouse_id
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_move_type_count, 0) AS move_move_type_count
    FROM deduped s
    LEFT JOIN move_stats st ON st.warehouse_id = s.id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY YEAR(opened_on), opened_on_month ORDER BY opened_on, id) AS rn,
        DATEDIFF(opened_on, LAG(opened_on) OVER w_opened_on_month_by_move_move) AS days_since_prev_opened_on,
        COUNT(*) OVER (PARTITION BY move_move_type_count, YEAR(opened_on)) AS rows_in_move_move_type_count_opened_on_year,
        ROUND(AVG(move_move_type_count) OVER (PARTITION BY opened_on_month ORDER BY move_move_type_count, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS move_move_type_count_avg_2,
        ROUND(capacity_bucket - AVG(capacity_bucket) OVER w_move_move_type_count_opened, 2) AS capacity_bucket_vs_move_move_type_count_opened_o
    FROM with_stats
    WINDOW w_opened_on_month_by_move_move AS (PARTITION BY opened_on_month ORDER BY move_move_type_count, id), w_move_move_type_count_opened AS (PARTITION BY move_move_type_count, YEAR(opened_on))
),
city_stats AS (
    -- Median opened_on_month per city
    SELECT city, PERCENTILE_APPROX(opened_on_month, 0.5) AS median_opened_on_month
    FROM with_trends
    GROUP BY city
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_opened_on_month,
        ROUND(s.opened_on_month - g.median_opened_on_month, 2) AS opened_on_month_minus_median
    FROM with_trends s
    JOIN city_stats g ON g.city = s.city
    WHERE s.opened_on_month >= g.median_opened_on_month
),
screened AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM outliers s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.id
        )
)
-- Final projection
SELECT
    id,
    wh_code,
    capacity,
    opened_on,
    capacity_bucket,
    wh_code_clean,
    opened_on_month,
    rn,
    days_since_prev_opened_on,
    rows_in_move_move_type_count_opened_on_year,
    capacity_bucket_vs_move_move_type_count_opened_o,
    opened_on_month_minus_median
FROM screened
ORDER BY capacity_bucket DESC, wh_code_clean
