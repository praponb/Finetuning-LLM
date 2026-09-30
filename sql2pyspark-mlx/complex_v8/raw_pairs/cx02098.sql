WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM warehouses
    WHERE city IN ('Kuala Lumpur', 'Bangkok')
),
without_matches AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM cleaned s
    WHERE s.id IN (
            SELECT st.warehouse_id
            FROM stock_moves st
            WHERE st.sku_id IS NOT NULL
        )
      AND s.city = 'Kuala Lumpur'
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY capacity ASC, id) AS capacity_seq,
        ROUND(capacity / NULLIF(SUM(capacity) OVER (PARTITION BY YEAR(opened_on), city), 0), 4) AS capacity_share_of_opened_on_year_city,
        SUM(capacity) OVER (PARTITION BY YEAR(opened_on), city) AS opened_on_year_city_sum_capacity,
        MIN(capacity) OVER (PARTITION BY YEAR(opened_on), city) AS opened_on_year_city_min_capacity,
        ROUND(AVG(capacity) OVER (PARTITION BY city ORDER BY capacity ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS capacity_avg_3
    FROM without_matches
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(capacity) OVER (ORDER BY capacity_share_of_opened_on_year_city DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS capacity_avg_2,
        DENSE_RANK() OVER w_all_by_capacity_share_of_ope_2 AS capacity_share_of_opened_on_year_city_dense_rank
    FROM metrics
    WINDOW w_all_by_capacity_share_of_ope_2 AS (ORDER BY capacity_share_of_opened_on_year_city DESC)
)
SELECT
    id,
    wh_code,
    city,
    capacity,
    opened_on,
    capacity_seq,
    capacity_share_of_opened_on_year_city,
    opened_on_year_city_sum_capacity,
    opened_on_year_city_min_capacity,
    capacity_avg_2,
    CONCAT(city, '#', CAST(capacity_seq AS STRING)) AS city_ref
FROM sequenced
