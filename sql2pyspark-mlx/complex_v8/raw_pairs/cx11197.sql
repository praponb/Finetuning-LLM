WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        FLOOR(capacity / 5000) * 5000 AS capacity_bucket,
        COALESCE(capacity, 0) AS capacity_filled,
        LENGTH(TRIM(wh_code)) AS wh_code_len
    FROM warehouses
    WHERE city = 'Singapore'
      AND opened_on IS NOT NULL
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        UPPER(TRIM(city)) AS city_clean,
        SUBSTRING(TRIM(wh_code), 1, 2) AS wh_code_prefix
    FROM scoped
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(capacity) OVER (PARTITION BY city, wh_code_len) AS city_wh_code_len_sum_capacity,
        capacity_filled - LAG(capacity_filled) OVER (PARTITION BY wh_code_len, DATE_TRUNC('MONTH', opened_on) ORDER BY capacity_bucket DESC, id) AS capacity_filled_change,
        MAX(capacity_filled) OVER (PARTITION BY city ORDER BY capacity_bucket, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS capacity_filled_max_1_prior,
        LAST_VALUE(opened_on) OVER (PARTITION BY city ORDER BY capacity_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_opened_on,
        ROUND(wh_code_len / NULLIF(SUM(wh_code_len) OVER (PARTITION BY city, wh_code_len), 0), 4) AS wh_code_len_share_of_city_wh_code_len,
        SUM(capacity_bucket) OVER (PARTITION BY city, wh_code_len) AS city_wh_code_len_sum_capacity_bucket
    FROM scored
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(city, 'UNKNOWN') AS city_or_unknown,
        ROUND(capacity / NULLIF(capacity_filled, 0), 3) AS capacity_per_capacity_filled
    FROM with_windows
),
city_or_unknown_stats AS (
    -- Avg capacity per city_or_unknown
    SELECT city_or_unknown, AVG(capacity) AS avg_capacity
    FROM tagged
    GROUP BY city_or_unknown
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_capacity,
        ROUND(s.capacity - g.avg_capacity, 2) AS capacity_minus_avg
    FROM tagged s
    JOIN city_or_unknown_stats g ON g.city_or_unknown = s.city_or_unknown
    WHERE s.capacity > g.avg_capacity
),
without_matches AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM compared s
    WHERE s.id IN (
            SELECT st.warehouse_id
            FROM stock_moves st
        )
)
-- Final projection
SELECT
    id,
    wh_code,
    city,
    capacity,
    opened_on,
    capacity_bucket,
    wh_code_len,
    city_clean,
    wh_code_prefix,
    capacity_filled_change,
    capacity_filled_max_1_prior,
    last_opened_on,
    wh_code_len_share_of_city_wh_code_len,
    city_wh_code_len_sum_capacity_bucket,
    city_or_unknown,
    capacity_per_capacity_filled,
    avg_capacity,
    capacity_minus_avg,
    ROUND(capacity / NULLIF(capacity_filled, 0), 3) AS capacity_per_capacity_filled_2,
    ABS(capacity_filled_change - 0) AS capacity_filled_change_distance
FROM without_matches
ORDER BY city
