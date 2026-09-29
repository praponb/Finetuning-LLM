WITH cleaned_warehouses AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        CASE WHEN capacity >= 11506 THEN 'A' WHEN capacity >= 4912 THEN 'B' ELSE 'C' END AS capacity_tier,
        TRUNC(opened_on, 'MM') AS opened_on_month_start,
        CASE WHEN capacity >= 11506 THEN 'A' WHEN capacity >= 4912 THEN 'B' ELSE 'C' END AS capacity_tier_2
    FROM warehouses
    WHERE UPPER(TRIM(wh_code)) LIKE 'B%'
      AND opened_on >= DATE '2024-04-13'
),
with_activity AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM cleaned_warehouses s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.id
              AND st.move_type IN ('ADJUST', 'OUTBOUND')
              AND st.moved_at >= TIMESTAMP '2025-01-19 12:00:00'
        )
      AND s.opened_on_month_start IS NOT NULL
)
SELECT
    id,
    wh_code,
    capacity,
    opened_on,
    capacity_tier,
    opened_on_month_start,
    DATEDIFF(DATE '2025-12-31', opened_on) AS days_since_opened_on
FROM with_activity
ORDER BY opened_on, opened_on_month_start DESC NULLS FIRST
