WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN credit_limit >= 14200 THEN 'High' WHEN credit_limit >= 6200 THEN 'Medium' ELSE 'Low' END credit_limit_band,
        COALESCE(city, 'N/A') city_filled
    FROM customers
    WHERE signup_date >= DATE '2023-08-27'
      AND segment IS NOT NULL
      AND age BETWEEN 31 AND 38
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT_WS('-', name, city) name_city_key,
        ROUND(credit_limit * 0.85, 2) adjusted_credit_limit
    FROM cleaned
    WHERE signup_date >= DATE '2024-02-14'
      AND country = 'SG'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(4) OVER (ORDER BY age, id) age_bucket,
        SUM(credit_limit) OVER (PARTITION BY city, segment ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_credit_limit,
        NTILE(4) OVER (ORDER BY adjusted_credit_limit, id) adjusted_credit_limit_bucket,
        MIN(adjusted_credit_limit) OVER (PARTITION BY country) country_min_adjusted_credit_limit,
        NTILE(10) OVER (PARTITION BY country ORDER BY age DESC, id) age_bucket_2
    FROM valid
),
enriched_customers AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(adjusted_credit_limit_bucket / SUM(adjusted_credit_limit_bucket) OVER (PARTITION BY city, country), 4) adjusted_credit_limit_bucket_share,
        LAST_VALUE(age_bucket_2, TRUE) OVER (PARTITION BY city ORDER BY adjusted_credit_limit_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_age_bucket_2
    FROM enriched
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY adjusted_credit_limit_bucket_share NULLS LAST, id) AS row_num
        FROM enriched_customers
    ) ranked
    WHERE row_num = 1
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY country ORDER BY signup_date DESC) AS latest_rank
    FROM ranked
    QUALIFY latest_rank = 1
)
SELECT
    id,
    email,
    segment,
    is_active,
    credit_limit,
    credit_limit_band,
    city_filled,
    adjusted_credit_limit,
    running_credit_limit
FROM latest
ORDER BY segment NULLS FIRST
