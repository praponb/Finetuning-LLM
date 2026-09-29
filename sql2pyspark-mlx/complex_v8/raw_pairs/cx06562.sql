WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        wh_code,
        city,
        capacity,
        QUARTER(opened_on) AS opened_on_quarter
    FROM warehouses
    WHERE city NOT IN ('Singapore', 'Bangkok')
      AND opened_on < DATE '2025-07-03'
),
screened AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.id
              AND st.unit_price < 289.7
              AND st.move_type = 'OUTBOUND'
        )
      AND s.city = 'Bangkok'
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT_WS('|', city, wh_code) AS city_wh_code_label,
        opened_on_quarter - capacity AS opened_on_quarter_minus_capacity
    FROM screened
    WHERE city = 'Singapore'
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on_quarter,
        opened_on_quarter_minus_capacity,
        COUNT(*) OVER w_city AS rows_in_city,
        ROUND(opened_on_quarter - AVG(opened_on_quarter) OVER w_city, 2) AS opened_on_quarter_vs_city_avg,
        MIN(opened_on_quarter) OVER (PARTITION BY opened_on_quarter, city ORDER BY opened_on_quarter ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_opened_on_quarter_so_far,
        LAG(capacity, 3, 0) OVER (PARTITION BY opened_on_quarter, city ORDER BY opened_on_quarter ASC, id) AS prev3_capacity
    FROM enriched
    WINDOW w_city AS (PARTITION BY city)
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        wh_code,
        capacity,
        opened_on_quarter_minus_capacity,
        rows_in_city,
        min_opened_on_quarter_so_far,
        prev3_capacity,
        ROUND(opened_on_quarter / NULLIF(SUM(opened_on_quarter) OVER w_opened_on_quarter, 0), 4) AS opened_on_quarter_share_of_opened_on_quarter,
        SUM(prev3_capacity) OVER (ORDER BY opened_on_quarter_minus_capacity ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS prev3_capacity_sum_6_prior
    FROM windowed
    WINDOW w_opened_on_quarter AS (PARTITION BY opened_on_quarter)
)
-- Final projection
SELECT
    id,
    wh_code,
    capacity,
    opened_on_quarter_minus_capacity,
    rows_in_city,
    min_opened_on_quarter_so_far,
    opened_on_quarter_share_of_opened_on_quarter,
    prev3_capacity_sum_6_prior,
    CONCAT(wh_code, '#', CAST(prev3_capacity AS STRING)) AS wh_code_ref
FROM with_windows
WHERE wh_code IS NOT NULL
ORDER BY wh_code_ref DESC NULLS FIRST, min_opened_on_quarter_so_far DESC NULLS FIRST
