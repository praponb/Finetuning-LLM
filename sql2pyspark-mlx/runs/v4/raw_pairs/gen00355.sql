WITH kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM customers
    EXCEPT ALL
    SELECT id
    FROM customers
    WHERE name LIKE 'He%'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM customers s
    INNER JOIN kept_keys k ON s.id = k.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CORR(credit_limit, age) OVER (PARTITION BY city) AS corr_credit_limit_age,
        RANK() OVER (ORDER BY credit_limit DESC NULLS LAST) AS credit_limit_rank,
        DENSE_RANK() OVER (PARTITION BY segment ORDER BY credit_limit DESC NULLS FIRST) AS credit_limit_dense_rank
    FROM verified
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        corr_credit_limit_age,
        credit_limit_rank,
        credit_limit_dense_rank,
        COUNT(corr_credit_limit_age) OVER (PARTITION BY country) AS country_count_corr_credit_limit_age,
        MIN(corr_credit_limit_age) OVER (PARTITION BY segment) AS segment_min_corr_credit_limit_age
    FROM scored
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions x
            WHERE x.customer_id = s.id
              AND x.country = 'US'
        )
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        INITCAP(TRIM(country)) AS country_title,
        COALESCE(city, 'N/A') AS city_clean
    FROM eligible
    WHERE signup_date > DATE '2023-05-13'
      AND city IS NOT NULL
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions x
            WHERE x.customer_id = s.id
              AND x.country IN ('DE', 'UK', 'JP')
        )
),
daily AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('YEAR', signup_date) AS year_start,
        DATE_TRUNC('WEEK', signup_date) AS week_start,
        MAX(country_count_corr_credit_limit_age) FILTER (WHERE segment = 'SMB') AS smb_max_country_count_corr_credit_limit_age,
        AVG(credit_limit_rank) FILTER (WHERE country = 'UK') AS uk_avg_credit_limit_rank,
        COUNT(DISTINCT city) AS distinct_cities,
        SORT_ARRAY(COLLECT_SET(segment)) AS segment_list
    FROM kept
    GROUP BY DATE_TRUNC('YEAR', signup_date), DATE_TRUNC('WEEK', signup_date)
)
SELECT
    year_start,
    week_start,
    smb_max_country_count_corr_credit_limit_age,
    uk_avg_credit_limit_rank,
    distinct_cities,
    segment_list
FROM daily
