WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        CASE WHEN city IN ('Singapore', 'Bangkok') THEN 'group_a' WHEN city = 'Kuala Lumpur' THEN 'group_b' ELSE 'other' END AS city_group,
        CASE WHEN capacity >= 11506 THEN 'A' WHEN capacity >= 11005 THEN 'B' ELSE 'C' END AS capacity_tier,
        QUARTER(opened_on) AS opened_on_quarter
    FROM warehouses
    WHERE opened_on < DATE '2025-02-14'
      AND capacity < 11768
),
with_activity AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.id
              AND st.move_type = 'TRANSFER'
        )
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY opened_on_quarter) AS rows_in_opened_on_quarter,
        ROUND(AVG(capacity) OVER (PARTITION BY capacity_tier, city ORDER BY opened_on_quarter, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS capacity_avg_6_prior,
        ROUND(capacity / NULLIF(SUM(capacity) OVER (PARTITION BY opened_on_quarter), 0), 4) AS capacity_share_of_opened_on_quarter,
        MAX(capacity) OVER (PARTITION BY opened_on_quarter) AS opened_on_quarter_max_capacity,
        ROUND(AVG(capacity) OVER (PARTITION BY capacity_tier, city ORDER BY opened_on_quarter, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS capacity_avg_7
    FROM with_activity
),
qualified AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM sequenced s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.id
              AND st.operator IN ('op_dew', 'op_chan', 'op_eve')
              AND HOUR(st.moved_at) BETWEEN 10 AND 15
        )
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE st.move_type IN ('INBOUND', 'TRANSFER')) FROM stock_moves st WHERE st.warehouse_id = s.id) AS move_move_type_count
    FROM qualified s
),
rollup_base AS (
    -- Group-level totals
    SELECT
        capacity_tier,
        ROUND(100 * COUNT(CASE WHEN city NOT IN ('Bangkok', 'Singapore') THEN 1 END) / COUNT(*), 1) AS pct_city_not_in_bangkok_singapor,
        MIN(capacity_avg_7) AS min_capacity_avg_7,
        COUNT(*) AS row_count,
        MAX(capacity_avg_6_prior) AS max_capacity_avg_6_prior,
        SUM(opened_on_quarter_max_capacity) FILTER (WHERE city = 'Kuala Lumpur') AS opened_on_quarter_max_capacity_city_kuala_lumpur
    FROM profiled
    GROUP BY capacity_tier
)
SELECT
    capacity_tier,
    pct_city_not_in_bangkok_singapor,
    row_count,
    CASE WHEN capacity_tier IN ('B', 'A') THEN 'group_a' WHEN capacity_tier = 'C' THEN 'group_b' ELSE 'other' END AS capacity_tier_group
FROM rollup_base
ORDER BY pct_city_not_in_bangkok_singapor DESC
