WITH filtered AS (
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
        age,
        CASE WHEN country = 'DE' THEN 1 ELSE 0 END AS is_de,
        ROUND(credit_limit / age, 2) AS credit_limit_per_age
    FROM customers
    WHERE signup_date >= DATE '2023-03-16'
      AND city IS NOT NULL
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY signup_date DESC NULLS FIRST, id ASC) AS dedup_rank
    FROM filtered
    QUALIFY dedup_rank = 1
),
with_last AS (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.session_country
    FROM latest s
    JOIN LATERAL (
            SELECT c.country AS session_country
            FROM sessions c
            WHERE c.customer_id = s.id
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        name,
        email,
        country,
        signup_date,
        is_active,
        credit_limit,
        age,
        credit_limit_per_age,
        dedup_rank,
        session_country,
        AVG(credit_limit) OVER (PARTITION BY city, country ORDER BY credit_limit_per_age ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS credit_limit_avg_5,
        SUM(age) OVER (PARTITION BY session_country ORDER BY is_de ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_age
    FROM with_last
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('YEAR', signup_date) AS year_start,
    country,
    SUM(CASE WHEN session_country = 'US' THEN age ELSE 0 END) AS us_age,
    COUNT_IF(country = 'US') AS us_events,
    CORR(credit_limit, age) AS corr_credit_limit_age
FROM with_windows
GROUP BY ROLLUP (DATE_TRUNC('YEAR', signup_date), country)
HAVING SUM(CASE WHEN session_country = 'US' THEN age ELSE 0 END) < 1000000
ORDER BY corr_credit_limit_age DESC NULLS FIRST;
