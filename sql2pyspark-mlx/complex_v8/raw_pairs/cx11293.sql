WITH cleaned AS (
    -- Filter warehouses
    SELECT
        id,
        wh_code,
        city,
        capacity,
        ADD_MONTHS(opened_on, 1) AS opened_on_plus_1m,
        CONCAT_WS(' / ', wh_code, city) AS wh_code_city_label,
        CONCAT_WS('|', wh_code, city) AS wh_code_city_label_2
    FROM warehouses
    WHERE UPPER(TRIM(wh_code)) LIKE 'B%'
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM cleaned
    WHERE capacity > (SELECT AVG(capacity) FROM cleaned)
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        opened_on_plus_1m,
        wh_code_city_label,
        wh_code_city_label_2,
        SUM(capacity) OVER w_city AS city_sum_capacity,
        RANK() OVER (PARTITION BY city ORDER BY capacity, id) AS capacity_rank,
        MIN(capacity) OVER (PARTITION BY city ORDER BY capacity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_capacity_so_far,
        MAX(capacity) OVER (PARTITION BY city ORDER BY capacity, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS capacity_max_1_prior,
        ROUND(capacity / NULLIF(SUM(capacity) OVER (PARTITION BY city), 0), 4) AS capacity_share_of_city,
        FIRST_VALUE(capacity) IGNORE NULLS OVER (PARTITION BY city ORDER BY capacity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_capacity
    FROM qualified
    WINDOW w_city AS (PARTITION BY city)
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(first_known_capacity) OVER (PARTITION BY capacity_rank ORDER BY capacity_max_1_prior, id) AS next_first_known_capacity,
        NTILE(10) OVER (PARTITION BY capacity_rank ORDER BY capacity_max_1_prior, id) AS capacity_max_1_prior_decile,
        DENSE_RANK() OVER (PARTITION BY capacity_rank ORDER BY capacity_max_1_prior, id) AS capacity_max_1_prior_dense_rank,
        SUM(first_known_capacity) OVER (PARTITION BY capacity_rank ORDER BY capacity_max_1_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_first_known_capacity,
        MIN(first_known_capacity) OVER w_capacity_rank AS capacity_rank_min_first_known_capacity
    FROM windowed
    WINDOW w_capacity_rank AS (PARTITION BY capacity_rank)
),
sequenced_2 AS (
    -- Previous / next values per key
    SELECT
        id,
        opened_on_plus_1m,
        wh_code_city_label,
        city_sum_capacity,
        capacity_rank,
        capacity_max_1_prior,
        capacity_share_of_city,
        first_known_capacity,
        next_first_known_capacity,
        capacity_max_1_prior_decile,
        capacity_max_1_prior_dense_rank,
        running_first_known_capacity,
        capacity_rank_min_first_known_capacity,
        LAG(min_capacity_so_far, 3) OVER w_capacity_max_1_prior_decile AS prev3_min_capacity_so_far,
        COUNT(*) OVER (PARTITION BY capacity_max_1_prior_decile) AS rows_in_capacity_max_1_prior_decile,
        SUM(city_sum_capacity) OVER (PARTITION BY capacity_max_1_prior_decile) AS capacity_max_1_prior_decile_sum_city_sum_capacit,
        RANK() OVER (PARTITION BY capacity_max_1_prior_decile ORDER BY capacity_share_of_city NULLS LAST, id) AS capacity_share_of_city_rank
    FROM sequenced
    WINDOW w_capacity_max_1_prior_decile AS (PARTITION BY capacity_max_1_prior_decile ORDER BY capacity_share_of_city NULLS LAST, id)
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        opened_on_plus_1m,
        wh_code_city_label,
        city_sum_capacity,
        capacity_max_1_prior,
        capacity_share_of_city,
        first_known_capacity,
        next_first_known_capacity,
        running_first_known_capacity,
        rows_in_capacity_max_1_prior_decile,
        capacity_max_1_prior_decile_sum_city_sum_capacit,
        capacity_share_of_city_rank,
        LEAST(city_sum_capacity, capacity_share_of_city) AS least_city_sum_capacity_capacity_share_of_city
    FROM sequenced_2
),
filtered AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.id
              AND st.qty < 415
        )
      AND s.running_first_known_capacity > 30850
)
SELECT id, metric, amount
FROM (
    SELECT id, least_city_sum_capacity_capacity_share_of_city, capacity_share_of_city
    FROM filtered
) src
UNPIVOT (
    amount FOR metric IN (least_city_sum_capacity_capacity_share_of_city, capacity_share_of_city)
) unp
