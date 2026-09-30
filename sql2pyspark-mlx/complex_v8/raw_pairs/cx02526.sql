WITH base_warehouses AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on
    FROM warehouses
    WHERE city <> 'Kuala Lumpur'
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        DATE_FORMAT(opened_on, 'yyyy-MM') AS opened_on_label
    FROM base_warehouses
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        wh_code,
        city,
        opened_on,
        opened_on_label,
        RANK() OVER w_city_by_capacity_ties AS capacity_rank,
        FIRST_VALUE(capacity) OVER (PARTITION BY city ORDER BY opened_on ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_capacity,
        RANK() OVER w_city_by_opened_on_ties AS opened_on_rank,
        RANK() OVER w_city_by_opened_on AS opened_on_rank_2,
        ROUND(AVG(capacity) OVER (PARTITION BY city ORDER BY capacity, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS capacity_avg_7
    FROM tagged
    WINDOW w_city_by_capacity_ties AS (PARTITION BY city ORDER BY capacity), w_city_by_opened_on_ties AS (PARTITION BY city ORDER BY opened_on ASC), w_city_by_opened_on AS (PARTITION BY city ORDER BY opened_on ASC, id)
),
leaders AS (
    -- Top 3 per group
    SELECT *
    FROM sequenced
    WHERE capacity_rank <= 3
),
leaders_2 AS (
    -- Top 3 per group
    SELECT *
    FROM leaders
    WHERE opened_on_rank_2 <= 3
      AND opened_on_label IS NOT NULL
),
with_activity AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM leaders_2 s
    WHERE s.id IN (
            SELECT st.warehouse_id
            FROM stock_moves st
            WHERE st.operator = 'op_eve'
              AND st.qty <= 314
        )
      AND s.opened_on BETWEEN DATE '2024-02-19' AND DATE '2025-12-17'
)
-- Subtotals with GROUPING SETS
SELECT
    opened_on_rank_2,
    opened_on_rank,
    GROUPING_ID() AS grouping_level,
    SUM(opened_on_rank) AS total_opened_on_rank,
    COUNT(*) AS row_count
FROM with_activity
GROUP BY GROUPING SETS ((opened_on_rank_2, opened_on_rank), (opened_on_rank_2))
ORDER BY opened_on_rank_2 NULLS LAST, opened_on_rank NULLS LAST
