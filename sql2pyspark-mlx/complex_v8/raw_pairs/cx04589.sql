WITH filtered_warehouses AS (
    -- Scope the warehouses rows
    SELECT
        id,
        city,
        capacity,
        opened_on
    FROM warehouses
    WHERE wh_code IS NOT NULL
      AND opened_on < DATE '2025-02-14'
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        city,
        capacity,
        FIRST_VALUE(opened_on) OVER (PARTITION BY city ORDER BY opened_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_opened_on,
        COUNT(*) OVER (PARTITION BY city) AS rows_in_city,
        LAST_VALUE(capacity) IGNORE NULLS OVER (PARTITION BY city ORDER BY opened_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_capacity,
        ROW_NUMBER() OVER w_opened_on_year_by_opened_on AS seq_no,
        ROUND(CUME_DIST() OVER (PARTITION BY city ORDER BY opened_on DESC), 3) AS opened_on_cume_dist
    FROM filtered_warehouses
    WINDOW w_opened_on_year_by_opened_on AS (PARTITION BY YEAR(opened_on) ORDER BY opened_on ASC, id)
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        city,
        capacity,
        rows_in_city,
        seq_no,
        opened_on_cume_dist,
        ROUND(last_known_capacity / NULLIF(SUM(last_known_capacity) OVER (PARTITION BY city, seq_no), 0), 4) AS last_known_capacity_share_of_city_seq_no,
        SUM(rows_in_city) OVER w_city_seq_no AS city_seq_no_sum_rows_in_city,
        ROW_NUMBER() OVER w_all_by_last_known_capacity AS last_known_capacity_seq,
        ROUND(CUME_DIST() OVER (ORDER BY last_known_capacity DESC, id), 3) AS last_known_capacity_cume_dist,
        LAG(opened_on_cume_dist, 2) OVER (ORDER BY last_known_capacity DESC, id) AS prev2_opened_on_cume_dist
    FROM ranked
    WINDOW w_city_seq_no AS (PARTITION BY city, seq_no), w_all_by_last_known_capacity AS (ORDER BY last_known_capacity DESC, id)
),
qualified AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM windowed s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.id
              AND st.moved_at >= TIMESTAMP '2025-01-25 10:00:00'
        )
      AND s.last_known_capacity_cume_dist <= 0.8
)
SELECT id, field, value
FROM (
    SELECT id, opened_on_cume_dist, last_known_capacity_cume_dist, last_known_capacity_share_of_city_seq_no, prev2_opened_on_cume_dist
    FROM qualified
) src
UNPIVOT (
    value FOR field IN (opened_on_cume_dist AS opened_on_cume_dist_v, last_known_capacity_cume_dist AS m_last_known_capacity_cume_dist, last_known_capacity_share_of_city_seq_no AS last_known_capacity_share_of_city_seq_no_v, prev2_opened_on_cume_dist AS m_prev2_opened_on_cume_dist)
) unp
