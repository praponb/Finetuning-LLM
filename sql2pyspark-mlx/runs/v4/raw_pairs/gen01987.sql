WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY age, id) AS dedup_rank
        FROM customers
    ) x
    WHERE dedup_rank = 1
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        name,
        email,
        country,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        credit_limit * age AS credit_limit_times_age,
        COALESCE(city, 'DIRECT') AS city_clean
    FROM latest
    WHERE name LIKE '%Ross%'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        name,
        email,
        country,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        credit_limit_times_age,
        city_clean,
        NTILE(10) OVER (PARTITION BY segment ORDER BY credit_limit, id) AS credit_limit_bucket,
        CUME_DIST() OVER (PARTITION BY segment ORDER BY credit_limit_times_age DESC NULLS FIRST) AS credit_limit_times_age_cume_dist,
        LEAD(credit_limit_times_age, 2) OVER (PARTITION BY country ORDER BY credit_limit, id) AS next_credit_limit_times_age,
        SUM(age) OVER w_ordered AS cumulative_age
    FROM cleaned
    WINDOW w_ordered AS (PARTITION BY country ORDER BY credit_limit_times_age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
cleaned_customers AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        email,
        country,
        segment,
        signup_date,
        credit_limit,
        age,
        credit_limit_times_age,
        city_clean,
        credit_limit_bucket,
        credit_limit_times_age_cume_dist,
        next_credit_limit_times_age,
        cumulative_age,
        COALESCE(credit_limit_bucket, 0) AS credit_limit_bucket_filled,
        LENGTH(country) AS country_length
    FROM with_windows
    WHERE age = 38
),
with_windows_customers AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(credit_limit_bucket_filled) IGNORE NULLS OVER w_grp AS last_known_credit_limit_bucket_filled,
        RANK() OVER w_ordered AS credit_limit_rank,
        ROUND(cumulative_age / SUM(cumulative_age) OVER (PARTITION BY country), 4) AS cumulative_age_share
    FROM cleaned_customers
    WINDOW w_grp AS (PARTITION BY segment, country ORDER BY cumulative_age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY segment ORDER BY credit_limit NULLS FIRST)
),
latest_match AS (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.started_at
    FROM with_windows_customers s
    LEFT JOIN LATERAL (
            SELECT c.started_at
            FROM sessions c
            WHERE c.customer_id = s.id
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
-- Window metrics over several different partitions
SELECT
    id,
    email,
    country,
    segment,
    signup_date,
    credit_limit,
    age,
    credit_limit_times_age,
    city_clean,
    credit_limit_bucket,
    credit_limit_times_age_cume_dist,
    next_credit_limit_times_age,
    cumulative_age,
    credit_limit_bucket_filled,
    country_length,
    last_known_credit_limit_bucket_filled,
    credit_limit_rank,
    cumulative_age_share,
    started_at,
    NTILE(3) OVER (PARTITION BY segment ORDER BY credit_limit_bucket_filled, id) AS credit_limit_bucket_filled_bucket,
    MAX(last_known_credit_limit_bucket_filled) OVER (PARTITION BY segment) AS segment_max_last_known_credit_limit_bucket_filled
FROM latest_match;
