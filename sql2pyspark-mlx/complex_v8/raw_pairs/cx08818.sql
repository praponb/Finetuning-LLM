WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        wh_code,
        capacity,
        opened_on,
        CONCAT(wh_code, '#', CAST(id AS STRING)) AS wh_code_ref,
        DATE_FORMAT(opened_on, 'yyyy-MM') AS opened_on_label,
        SUBSTRING(TRIM(wh_code), 1, 4) AS wh_code_prefix
    FROM warehouses
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MIN(st.unit_price) FROM stock_moves st WHERE st.warehouse_id = s.id) AS min_move_unit_price,
        (SELECT SUM(st.qty) FROM stock_moves st WHERE st.warehouse_id = s.id) AS total_move_qty
    FROM scoped s
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        capacity,
        opened_on,
        opened_on_label,
        wh_code_prefix,
        min_move_unit_price,
        total_move_qty,
        ADD_MONTHS(opened_on, 1) AS opened_on_plus_1m
    FROM profiled
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        capacity,
        opened_on,
        opened_on_label,
        min_move_unit_price,
        total_move_qty,
        opened_on_plus_1m,
        SUM(total_move_qty) OVER (ORDER BY opened_on_plus_1m DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS total_move_qty_sum_7,
        ROUND(AVG(total_move_qty) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_on_plus_1m)), 2) AS opened_on_plus_1m_month_avg_total_move_qty
    FROM annotated
),
labeled AS (
    -- Derive helper columns
    SELECT *, SUBSTRING(TRIM(opened_on_label), 1, 4) AS opened_on_label_prefix
    FROM with_trends
    WHERE opened_on >= DATE '2024-11-25'
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        capacity,
        opened_on,
        opened_on_label,
        min_move_unit_price,
        total_move_qty,
        total_move_qty_sum_7,
        opened_on_plus_1m_month_avg_total_move_qty,
        opened_on_label_prefix,
        ABS(min_move_unit_price - 150.7) AS min_move_unit_price_distance
    FROM labeled
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN total_move_qty_sum_7 >= 7200 THEN 'A' WHEN total_move_qty_sum_7 >= 2701 THEN 'B' ELSE 'C' END AS total_move_qty_sum_7_tier,
        YEAR(opened_on) AS opened_on_year,
        MAX(opened_on) AS last_opened_on,
        PERCENTILE_APPROX(capacity, 0.5) AS median_capacity,
        COUNT(*) AS n_rows,
        ROUND(SUM(min_move_unit_price_distance) / NULLIF(SUM(opened_on_plus_1m_month_avg_total_move_qty), 0), 3) AS min_move_unit_price_distance_to_opened_on_plus_1,
        COUNT(DISTINCT id) AS distinct_ids
    FROM tagged
    GROUP BY CASE WHEN total_move_qty_sum_7 >= 7200 THEN 'A' WHEN total_move_qty_sum_7 >= 2701 THEN 'B' ELSE 'C' END, YEAR(opened_on)
)
-- Final projection
SELECT
    total_move_qty_sum_7_tier,
    last_opened_on,
    median_capacity,
    n_rows,
    min_move_unit_price_distance_to_opened_on_plus_1,
    distinct_ids,
    DATE_FORMAT(last_opened_on, 'yyyy-MM') AS last_opened_on_label
FROM grouped
WHERE last_opened_on < DATE '2025-07-03'
ORDER BY min_move_unit_price_distance_to_opened_on_plus_1 DESC
