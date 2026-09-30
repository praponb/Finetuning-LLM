WITH cleaned_warehouses AS (
    -- Scope the warehouses rows
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        DATE_FORMAT(opened_on, 'yyyy-MM') AS opened_on_label,
        CONCAT_WS('-', city, wh_code) AS city_wh_code_label
    FROM warehouses
    WHERE wh_code IS NOT NULL
      AND capacity <= 11005
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        opened_on_label,
        city_wh_code_label,
        SUM(capacity) OVER w_opened_on_month_city AS opened_on_month_city_sum_capacity,
        SUM(capacity) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_on), city ORDER BY opened_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_capacity,
        ROUND(capacity - AVG(capacity) OVER w_opened_on_month_city, 2) AS capacity_vs_opened_on_month_city_avg,
        MAX(capacity) OVER (PARTITION BY city ORDER BY opened_on DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS capacity_max_2_prior,
        FIRST_VALUE(opened_on) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', opened_on), city ORDER BY opened_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_opened_on,
        ROUND(capacity / NULLIF(SUM(capacity) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_on), city), 0), 4) AS capacity_share_of_opened_on_month_city
    FROM cleaned_warehouses
    WINDOW w_opened_on_month_city AS (PARTITION BY DATE_TRUNC('MONTH', opened_on), city)
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        city,
        capacity,
        opened_on,
        city_wh_code_label,
        opened_on_month_city_sum_capacity,
        capacity_vs_opened_on_month_city_avg,
        capacity_max_2_prior,
        capacity_share_of_opened_on_month_city,
        SUM(capacity_max_2_prior) OVER (PARTITION BY YEAR(first_known_opened_on), city) AS first_known_opened_on_year_city_sum_capacity_max,
        LAG(capacity_max_2_prior, 3, 0) OVER (PARTITION BY DATE_TRUNC('MONTH', first_known_opened_on), city ORDER BY opened_on_month_city_sum_capacity, id) AS prev3_capacity_max_2_prior
    FROM windowed
),
with_children_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.city,
        s.opened_on_month_city_sum_capacity,
        s.first_known_opened_on_year_city_sum_capacity_max,
        s.prev3_capacity_max_2_prior,
        st.id AS move_id,
        st.move_type,
        st.moved_at AS move_moved_at
    FROM with_trends s
    LEFT JOIN stock_moves st ON st.warehouse_id = s.id
)
SELECT
    move_id,
    city,
    GROUPING(move_id) AS is_total_move_id,
    SUM(opened_on_month_city_sum_capacity) FILTER (WHERE move_type = 'INBOUND') AS opened_on_month_city_sum_capacity_move_type_inbo
FROM with_children_moves
GROUP BY GROUPING SETS ((move_id, city), (move_id), ())
ORDER BY move_id NULLS LAST, city NULLS LAST
