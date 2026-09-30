WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LOWER(segment) AS segment_lower,
        CASE WHEN city = 'Bangkok' THEN 1 ELSE 0 END AS is_bangkok
    FROM customers
    WHERE city IS NOT NULL
      AND age = 64
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY country, segment ORDER BY is_bangkok DESC, id) AS is_bangkok_bucket,
        MAX(is_bangkok) OVER w_recent AS city_max_is_bangkok,
        ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY country, segment), 4) AS credit_limit_share
    FROM valid
    WINDOW w_recent AS (PARTITION BY city, country)
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT segment, country
    FROM with_windows
    INTERSECT ALL
    SELECT segment, country
    FROM customers
    WHERE name LIKE 'P%'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN remaining_keys k ON s.segment = k.segment AND s.country = k.country
),
with_windows_customers AS (
    -- Running and moving aggregates
    SELECT
        id,
        city,
        signup_date,
        is_active,
        age,
        segment_lower,
        is_bangkok,
        is_bangkok_bucket,
        credit_limit_share,
        ROUND(is_bangkok_bucket / SUM(is_bangkok_bucket) OVER (PARTITION BY city), 4) AS is_bangkok_bucket_share,
        ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY YEAR(signup_date), segment), 4) AS credit_limit_share_2,
        STDDEV_SAMP(is_bangkok_bucket) OVER w_part AS city_stddev_samp_is_bangkok_bucket,
        DENSE_RANK() OVER (PARTITION BY segment, city ORDER BY is_bangkok_bucket DESC NULLS FIRST) AS is_bangkok_bucket_dense_rank,
        SUM(age) OVER w_running AS signup_date_year_sum_age
    FROM reconciled
    WINDOW w_part AS (PARTITION BY city, country), w_running AS (PARTITION BY YEAR(signup_date), segment)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        city,
        signup_date,
        is_active,
        is_bangkok_bucket,
        is_bangkok_bucket_share,
        RANK() OVER (PARTITION BY YEAR(signup_date), city ORDER BY age DESC) AS age_rank,
        AVG(is_bangkok_bucket) OVER (PARTITION BY city) AS city_avg_is_bangkok_bucket
    FROM with_windows_customers
    WHERE city_stddev_samp_is_bangkok_bucket <> 5
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        city,
        signup_date,
        is_active,
        is_bangkok_bucket,
        is_bangkok_bucket_share,
        age_rank,
        city_avg_is_bangkok_bucket,
        SUBSTRING(city, 1, 4) AS city_prefix,
        COALESCE(city, 'NONE') AS city_clean
    FROM scored
    WHERE signup_date >= DATE '2023-02-27'
      AND city IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        city,
        signup_date,
        city_prefix,
        PERCENT_RANK() OVER (PARTITION BY city ORDER BY city_avg_is_bangkok_bucket DESC) AS city_avg_is_bangkok_bucket_percent_rank,
        SUM(age_rank) OVER (PARTITION BY city) AS city_sum_age_rank
    FROM base
)
SELECT
    city,
    signup_date,
    city_prefix,
    city_avg_is_bangkok_bucket_percent_rank,
    city_sum_age_rank
FROM enriched
ORDER BY signup_date, city;
