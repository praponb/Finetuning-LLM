WITH scoped AS (
    -- Scope the warehouses rows
    SELECT
        id,
        wh_code,
        capacity,
        opened_on,
        DATE_FORMAT(opened_on, 'yyyy-MM-dd') AS opened_on_label
    FROM warehouses
    WHERE city <> 'Singapore'
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        capacity,
        opened_on,
        opened_on_label,
        CASE WHEN capacity >= 11506 THEN 'HIGH' WHEN capacity >= 4912 THEN 'MEDIUM' ELSE 'LOW' END AS capacity_tier,
        COALESCE(capacity, 0) AS capacity_filled
    FROM scoped
),
latest_per_key AS (
    -- Keep the latest row per capacity_tier
    SELECT
        id,
        capacity,
        opened_on,
        opened_on_label,
        capacity_tier,
        capacity_filled
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY capacity_tier ORDER BY opened_on DESC, id DESC) AS row_num
        FROM scored
    ) ranked
    WHERE row_num = 1
),
screened AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM latest_per_key s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.id
              AND st.operator IS NOT NULL
              AND HOUR(st.moved_at) BETWEEN 10 AND 12
        )
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        capacity,
        opened_on_label,
        capacity_tier,
        capacity_filled,
        SUM(capacity) OVER (PARTITION BY MONTH(opened_on)) AS opened_on_month_sum_capacity,
        ROUND(AVG(capacity) OVER w_opened_on_month, 2) AS opened_on_month_avg_capacity,
        ROUND(capacity - AVG(capacity) OVER (PARTITION BY MONTH(opened_on)), 2) AS capacity_vs_opened_on_month_avg,
        LAST_VALUE(opened_on) IGNORE NULLS OVER (ORDER BY capacity DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_opened_on,
        LEAD(capacity) OVER (ORDER BY capacity_filled DESC, id) AS next_capacity
    FROM screened
    WINDOW w_opened_on_month AS (PARTITION BY MONTH(opened_on))
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MIN(st.moved_at) FROM stock_moves st WHERE st.warehouse_id = s.id) AS first_moved_at,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.warehouse_id = s.id) AS move_count
    FROM with_windows s
)
SELECT
    id,
    capacity,
    opened_on_label,
    capacity_tier,
    opened_on_month_sum_capacity,
    opened_on_month_avg_capacity,
    capacity_vs_opened_on_month_avg,
    last_known_opened_on,
    next_capacity,
    first_moved_at,
    move_count,
    DAYOFWEEK(first_moved_at) AS first_moved_at_dayofweek,
    COALESCE(capacity_tier, 'UNKNOWN') AS capacity_tier_or_unknown
FROM profiled
ORDER BY opened_on_month_sum_capacity NULLS LAST, id NULLS LAST
LIMIT 20
