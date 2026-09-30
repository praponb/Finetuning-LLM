WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        name,
        email,
        country,
        city,
        signup_date,
        credit_limit,
        DATEDIFF(CURRENT_DATE(), signup_date) AS days_since_signup_date,
        ROUND(age * 1.1, 2) AS adjusted_age
    FROM customers
    WHERE segment IS NOT NULL
      AND city IS DISTINCT FROM 'Bangkok'
),
with_last AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.order_order_date
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.order_date AS order_order_date
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.discount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city, country ORDER BY days_since_signup_date DESC, id ASC) AS dedup_rank
    FROM with_last
    QUALIFY dedup_rank = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        name,
        city,
        signup_date,
        days_since_signup_date,
        adjusted_age,
        dedup_rank,
        NTILE(5) OVER (ORDER BY credit_limit ASC, id ASC) AS credit_limit_bucket,
        LAG(days_since_signup_date, 2) OVER (PARTITION BY country, city ORDER BY credit_limit ASC, id ASC) AS prev_days_since_signup_date,
        MIN(adjusted_age) OVER (PARTITION BY city, country) AS city_min_adjusted_age
    FROM latest
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY days_since_signup_date DESC, id ASC) AS row_num
        FROM enriched
    ) x
    WHERE row_num = 1
),
with_last_customers AS (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.session_country
    FROM ranked s
    JOIN LATERAL (
            SELECT c.country AS session_country
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.started_at <= s.signup_date
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    city,
    signup_date,
    days_since_signup_date,
    credit_limit_bucket,
    prev_days_since_signup_date,
    city_min_adjusted_age,
    row_num,
    session_country
FROM with_last_customers
ORDER BY prev_days_since_signup_date DESC NULLS FIRST, city_min_adjusted_age ASC;
