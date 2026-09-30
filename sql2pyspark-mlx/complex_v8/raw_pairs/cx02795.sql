WITH recent AS (
    -- Filter warehouses
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        LOWER(REGEXP_REPLACE(TRIM(city), ' ', '_')) AS city_slug,
        COALESCE(capacity, 0) AS capacity_filled
    FROM warehouses
    WHERE opened_on < DATE '2025-02-14'
      AND LOWER(wh_code) LIKE '%kk%'
),
deduped AS (
    -- Keep the latest row per city
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        city_slug,
        capacity_filled
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_on DESC, id DESC) AS recency_rank
        FROM recent
    ) ranked
    WHERE recency_rank = 1
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        LAG(capacity) OVER w_all_by_opened_on AS prev_capacity,
        ROUND(capacity_filled - AVG(capacity_filled) OVER (PARTITION BY city), 2) AS capacity_filled_vs_city_avg
    FROM deduped
    WINDOW w_all_by_opened_on AS (ORDER BY opened_on ASC NULLS LAST, id)
),
deduped_2 AS (
    -- Keep the earliest row per city
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        city_slug,
        capacity_filled,
        prev_capacity,
        capacity_filled_vs_city_avg
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY city ORDER BY prev_capacity, id) AS row_num
        FROM ranked
    ) ranked
    WHERE row_num = 1
),
city_stats AS (
    -- Max capacity per city
    SELECT city, MAX(capacity) AS max_capacity
    FROM deduped_2
    GROUP BY city
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_capacity,
        ROUND(s.capacity - g.max_capacity, 2) AS capacity_minus_max
    FROM deduped_2 s
    JOIN city_stats g ON g.city = s.city
    WHERE s.capacity = g.max_capacity
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, reading
FROM (
    SELECT id, capacity_filled_vs_city_avg, capacity_minus_max
    FROM vs_group
) src
UNPIVOT (
    reading FOR field IN (capacity_filled_vs_city_avg AS m_capacity_filled_vs_city_avg, capacity_minus_max AS m_capacity_minus_max)
) unp
