WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM sessions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id, device ORDER BY started_at NULLS LAST, id) = 1
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN browser IN ('Chrome', 'Edge') THEN 'Core' ELSE 'Other' END AS browser_group,
        COALESCE(browser, 'OTHER') AS browser_filled
    FROM latest
    WHERE started_at >= DATE '2023-05-13'
      AND country = 'SG'
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.device,
        a.browser,
        a.country,
        b.country AS customer_country,
        b.credit_limit,
        b.age
    FROM base a
    LEFT JOIN customers b
        ON a.customer_id = b.id
),
joined AS (
    -- Attach customers attributes
    SELECT
        src.*,
        c.name,
        c.city,
        c.signup_date
    FROM with_dims src
    LEFT JOIN customers c
        ON src.customer_id <=> c.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(credit_limit) OVER (PARTITION BY customer_country, browser ORDER BY age, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_credit_limit,
        SUM(credit_limit) OVER (PARTITION BY customer_country, browser ORDER BY age, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_credit_limit,
        AVG(credit_limit) OVER (PARTITION BY customer_country, browser ORDER BY age, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_credit_limit
    FROM joined
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        country,
        DATE_TRUNC('YEAR', signup_date) AS year_start,
        SUM(age) FILTER (WHERE device = 'mobile') AS mobile_total_age,
        COUNT(*) FILTER (WHERE country = 'JP') AS jp_rows,
        ROUND(AVG(credit_limit), 2) AS avg_credit_limit
    FROM metrics
    GROUP BY country, DATE_TRUNC('YEAR', signup_date)
)
SELECT country, year_start
FROM totals
ORDER BY year_start DESC NULLS LAST;
