WITH remaining_keys AS (
    -- Set intersection on keys
    SELECT id, country
    FROM customers
    INTERSECT
    SELECT id, country
    FROM customers
    WHERE age < 57
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM customers s
    JOIN remaining_keys k ON s.id = k.id AND s.country = k.country
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(age / SUM(age) OVER (PARTITION BY country), 4) AS age_share,
        STDDEV_SAMP(age) OVER (PARTITION BY country) AS country_stddev_samp_age
    FROM reconciled
),
matched_keys AS (
    -- Set intersection on keys
    SELECT country
    FROM scored
    INTERSECT
    SELECT country
    FROM customers
    WHERE country = 'UK'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    JOIN matched_keys k ON s.country = k.country
)
SELECT
    id,
    email,
    city,
    segment,
    signup_date,
    is_active,
    credit_limit,
    country_stddev_samp_age,
    CASE WHEN segment RLIKE '^(SMB|Consumer)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS segment_zone
FROM verified
