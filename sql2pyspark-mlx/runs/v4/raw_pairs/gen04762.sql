WITH kept_keys AS (
    -- Set difference on keys
    SELECT country, id
    FROM customers
    EXCEPT ALL
    SELECT country, id
    FROM customers
    WHERE name NOT LIKE 'H%'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM customers s
    INNER JOIN kept_keys k ON s.country = k.country AND s.id = k.id
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        LENGTH(city) AS city_length
    FROM kept_rows
    WHERE city IS NOT NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        city_length,
        AVG(city_length) OVER (PARTITION BY segment) AS segment_avg_city_length,
        CUME_DIST() OVER (ORDER BY age DESC NULLS FIRST) AS age_cume_dist
    FROM filtered
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LOWER(segment) AS segment_lower,
        COALESCE(city, 'NONE') AS resolved_city
    FROM metrics
    WHERE country = 'US'
),
filtered_customers AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_ADD(signup_date, 18) AS signup_date_plus_18d,
        DATE_ADD(signup_date, 45) AS signup_date_plus_45d
    FROM cleaned
    WHERE segment IS NOT NULL
)
-- Running and moving aggregates
SELECT
    id,
    email,
    country,
    city,
    segment,
    signup_date,
    is_active,
    credit_limit,
    age,
    city_length,
    segment_avg_city_length,
    age_cume_dist,
    segment_lower,
    resolved_city,
    signup_date_plus_18d,
    signup_date_plus_45d,
    CUME_DIST() OVER w_grp AS age_cume_dist_cume_dist,
    DENSE_RANK() OVER w_running AS age_dense_rank
FROM filtered_customers
WINDOW w_grp AS (PARTITION BY segment, country ORDER BY age_cume_dist ASC NULLS FIRST), w_running AS (ORDER BY age DESC NULLS LAST)
