WITH cleaned AS (
    -- Scope the warehouses rows
    SELECT
        id,
        wh_code,
        city,
        opened_on,
        YEAR(opened_on) AS opened_on_year,
        FLOOR(capacity / 5000) * 5000 AS capacity_bucket
    FROM warehouses
    WHERE opened_on BETWEEN DATE '2024-04-13' AND DATE '2025-12-17'
),
labeled AS (
    -- Derive helper columns
    SELECT *, CONCAT(city, '#', CAST(id AS STRING)) AS city_ref
    FROM cleaned
    WHERE city NOT IN ('Singapore', 'Bangkok')
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        wh_code,
        opened_on_year,
        capacity_bucket,
        city_ref,
        DATE_TRUNC('WEEK', opened_on) AS opened_on_week
    FROM labeled
    WHERE YEAR(opened_on) = 2025
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MIN(st.qty) FROM stock_moves st WHERE st.warehouse_id = s.id) AS min_move_qty
    FROM derived s
),
grouped AS (
    -- Group-level totals
    SELECT
        YEAR(opened_on_week) AS opened_on_week_year,
        opened_on_year,
        ARRAY_SORT(COLLECT_SET(wh_code)) AS wh_code_list,
        ROUND(AVG(capacity_bucket), 2) AS avg_capacity_bucket,
        SUM(opened_on_year) AS total_opened_on_year,
        SUM(min_move_qty) AS total_min_move_qty,
        ROUND(100 * COUNT(CASE WHEN LOWER(wh_code) LIKE '%nx%' THEN 1 END) / COUNT(*), 1) AS pct_lower_wh_code_like_nx,
        COUNT(*) FILTER (WHERE UPPER(TRIM(wh_code)) LIKE 'C%') AS n_upper_trim_wh_code_like_c
    FROM summarized_rows
    GROUP BY 1, 2
)
-- Final projection
SELECT
    opened_on_week_year,
    opened_on_year,
    total_opened_on_year,
    pct_lower_wh_code_like_nx,
    n_upper_trim_wh_code_like_c,
    COALESCE(total_min_move_qty, 0) AS total_min_move_qty_filled,
    CASE WHEN opened_on_year >= 2025 THEN 'HIGH' WHEN opened_on_year >= 2024 THEN 'MEDIUM' ELSE 'LOW' END AS opened_on_year_tier
FROM grouped
ORDER BY pct_lower_wh_code_like_nx, opened_on_year_tier
