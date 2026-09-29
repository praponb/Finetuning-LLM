WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        name,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        CONCAT_WS('/', segment, city) AS segment_city_key,
        COALESCE(city, 'OTHER') AS city_filled
    FROM customers
    WHERE signup_date > DATE '2023-08-14'
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city, segment ORDER BY credit_limit ASC NULLS FIRST, id ASC) = 1
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        country,
        city,
        credit_limit,
        age,
        MIN(credit_limit) OVER (PARTITION BY segment, country ORDER BY credit_limit ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_credit_limit,
        AVG(credit_limit) OVER (PARTITION BY segment, country ORDER BY credit_limit ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_credit_limit,
        SUM(credit_limit) OVER (PARTITION BY segment, country ORDER BY credit_limit ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_credit_limit,
        ROUND(age / SUM(age) OVER (PARTITION BY city), 4) AS age_share
    FROM deduped
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, CONCAT_WS('-', country, city) AS country_city_key
    FROM scored
    WHERE city IS NOT NULL
      AND running_sum_credit_limit > 74000
)
-- Grouped metrics
SELECT
    city,
    CASE WHEN age >= 70 THEN 'PREMIUM' ELSE 'STANDARD' END AS age_tier,
    ROUND(SUM(running_sum_credit_limit) / NULLIF(COUNT(CASE WHEN country = 'US' THEN 1 END), 0), 2) AS running_sum_credit_limit_per_us,
    AVG(running_sum_credit_limit) FILTER (WHERE country = 'TH') AS th_avg_running_sum_credit_limit,
    ROUND(SUM(running_avg_credit_limit) / NULLIF(COUNT(CASE WHEN city = 'Singapore' THEN 1 END), 0), 2) AS running_avg_credit_limit_per_singapore,
    CORR(credit_limit, age) AS corr_credit_limit_age,
    ROUND(SUM(running_min_credit_limit) / NULLIF(COUNT(CASE WHEN country = 'JP' THEN 1 END), 0), 2) AS running_min_credit_limit_per_jp,
    COUNT_IF(city = 'New York') AS new_york_events,
    GROUPING_ID() AS gid
FROM filtered
GROUP BY CUBE (city, CASE WHEN age >= 70 THEN 'PREMIUM' ELSE 'STANDARD' END)
ORDER BY running_min_credit_limit_per_jp DESC
