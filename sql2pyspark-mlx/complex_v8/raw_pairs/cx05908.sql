WITH prepared AS (
    -- Filter warehouses
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        COALESCE(capacity, 0) AS capacity_filled,
        COALESCE(city, 'UNKNOWN') AS city_or_unknown
    FROM warehouses
    WHERE UPPER(TRIM(wh_code)) LIKE 'K%'
      AND city = 'Bangkok'
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        city_or_unknown,
        city,
        COUNT(*) AS cnt,
        ROUND(AVG(capacity_filled), 2) AS avg_capacity_filled
    FROM prepared
    WHERE capacity <= 11506
    GROUP BY 1, 2
    HAVING AVG(capacity) > 4912
),
derived AS (
    -- Add calculated columns
    SELECT *, LEAST(cnt, avg_capacity_filled) AS least_cnt_avg_capacity_filled
    FROM rollup_base
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM derived
    WHERE cnt > (SELECT AVG(cnt) FROM derived)
)
SELECT
    city_or_unknown,
    city,
    cnt,
    avg_capacity_filled,
    least_cnt_avg_capacity_filled,
    LENGTH(TRIM(city_or_unknown)) AS city_or_unknown_len
FROM screened
