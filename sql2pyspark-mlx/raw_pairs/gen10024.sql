WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(segment, 'Unknown') AS segment_filled,
        INITCAP(TRIM(email)) AS email_title,
        COALESCE(city, 'UNKNOWN') AS resolved_city
    FROM customers
    WHERE segment IS NOT NULL
      AND signup_date > DATE '2023-03-08'
),
kept_keys AS (
    -- Set difference on keys
    SELECT country
    FROM base
    EXCEPT
    SELECT country
    FROM customers
    WHERE credit_limit <= 9100
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    JOIN kept_keys k ON s.country = k.country
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CORR(credit_limit, age) OVER (PARTITION BY segment, city) AS corr_credit_limit_age,
        STDDEV_SAMP(credit_limit) OVER (PARTITION BY country, segment) AS country_stddev_samp_credit_limit,
        CORR(age, credit_limit) OVER (PARTITION BY segment) AS corr_age_credit_limit
    FROM kept_rows
),
enriched_customers AS (
    -- Ranking and gap features
    SELECT
        name,
        city,
        segment,
        credit_limit,
        age,
        segment_filled,
        email_title,
        resolved_city,
        corr_credit_limit_age,
        country_stddev_samp_credit_limit,
        CUME_DIST() OVER (ORDER BY corr_age_credit_limit DESC) AS corr_age_credit_limit_cume_dist,
        DENSE_RANK() OVER w_hist AS country_stddev_samp_credit_limit_dense_rank,
        RANK() OVER (ORDER BY corr_credit_limit_age DESC) AS corr_credit_limit_age_rank
    FROM enriched
    WINDOW w_hist AS (PARTITION BY segment ORDER BY country_stddev_samp_credit_limit DESC NULLS LAST)
)
SELECT
    name,
    segment,
    age,
    segment_filled,
    email_title,
    resolved_city,
    corr_credit_limit_age,
    country_stddev_samp_credit_limit,
    corr_age_credit_limit_cume_dist
FROM enriched_customers
ORDER BY segment DESC;
