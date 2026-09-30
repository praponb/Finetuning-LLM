WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), signup_date) AS days_since_signup_date,
        COALESCE(city, 'N/A') AS resolved_city
    FROM customers
    WHERE signup_date >= DATE '2023-09-28'
      AND country IN ('UK', 'JP')
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        name,
        email,
        country,
        city,
        signup_date,
        is_active,
        credit_limit,
        resolved_city,
        CASE WHEN credit_limit >= 15100 THEN 'High' WHEN credit_limit >= 4300 THEN 'Medium' ELSE 'Low' END AS credit_limit_band,
        FLOOR(days_since_signup_date) AS days_since_signup_date_whole
    FROM filtered
    WHERE signup_date >= DATE '2023-11-16'
      AND segment IS NOT NULL
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM subscriptions x
            WHERE x.customer_id = s.id
              AND x.end_date BETWEEN '2023-08-02' AND '2024-10-01'
        )
),
filtered_customers AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN days_since_signup_date_whole >= 74 THEN 'High' WHEN days_since_signup_date_whole >= 22 THEN 'Medium' ELSE 'Low' END AS days_since_signup_date_whole_band
    FROM eligible
    WHERE signup_date >= DATE '2023-03-24'
      AND city IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY city ORDER BY days_since_signup_date_whole) AS days_since_signup_date_whole_percent_rank,
        SUM(days_since_signup_date_whole) OVER (PARTITION BY country, city ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_days_since_signup_date_whole,
        NTILE(10) OVER w_running AS days_since_signup_date_whole_bucket,
        NTILE(5) OVER w_grp AS days_since_signup_date_whole_bucket_2
    FROM filtered_customers
    WINDOW w_running AS (ORDER BY days_since_signup_date_whole DESC, id), w_grp AS (PARTITION BY city ORDER BY days_since_signup_date_whole DESC, id)
)
SELECT
    id,
    name,
    email,
    country,
    city,
    is_active,
    credit_limit,
    days_since_signup_date_whole,
    days_since_signup_date_whole_percent_rank
FROM enriched
ORDER BY id DESC NULLS FIRST, country;
