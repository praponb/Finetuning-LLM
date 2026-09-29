WITH filtered AS (
    -- Scope the warehouses rows
    SELECT *
    FROM warehouses
    WHERE wh_code IS NOT NULL
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(capacity) OVER w_city AS city_max_capacity,
        ROUND(capacity - AVG(capacity) OVER w_city, 2) AS capacity_vs_city_avg,
        ROW_NUMBER() OVER w_city_by_capacity AS capacity_seq,
        DENSE_RANK() OVER w_city_by_capacity AS capacity_dense_rank,
        ROUND(PERCENT_RANK() OVER w_city_by_capacity, 3) AS capacity_percent_rank
    FROM filtered
    WINDOW w_city AS (PARTITION BY city), w_city_by_capacity AS (PARTITION BY city ORDER BY capacity, id)
),
leaders AS (
    -- Top 3 per group
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        city_max_capacity,
        capacity_vs_city_avg,
        capacity_seq,
        capacity_percent_rank
    FROM windowed
    WHERE capacity_dense_rank <= 3
      AND capacity_seq > 5
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LEAD(capacity_seq, 3, 0) OVER (PARTITION BY city, capacity_seq ORDER BY capacity, id) AS next3_capacity_seq,
        ROUND(capacity - AVG(capacity) OVER (PARTITION BY city), 2) AS capacity_vs_city_avg_2
    FROM leaders
)
-- Final projection
SELECT
    id,
    city,
    capacity,
    opened_on,
    city_max_capacity,
    capacity_vs_city_avg,
    capacity_seq,
    capacity_vs_city_avg_2,
    CONCAT_WS('|', wh_code, city) AS wh_code_city_label
FROM ranked
WHERE capacity_percent_rank >= 0.1
ORDER BY opened_on, capacity_seq
