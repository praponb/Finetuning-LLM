WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY country ORDER BY credit_limit NULLS LAST) AS latest_rank
        FROM customers
    ) ranked
    WHERE latest_rank = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(credit_limit) OVER (PARTITION BY segment ORDER BY credit_limit, id) AS next_credit_limit,
        LEAD(age) OVER (PARTITION BY segment ORDER BY credit_limit, id) AS next_age,
        LAG(age) OVER (PARTITION BY segment ORDER BY credit_limit, id) AS prev_age,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY credit_limit, id) AS event_seq
    FROM latest
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        credit_limit,
        latest_rank,
        next_credit_limit,
        prev_age,
        event_seq,
        ROUND(event_seq * 1.15, 2) AS event_seq_adjusted
    FROM with_windows
    WHERE signup_date >= DATE '2023-04-03'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        name,
        email,
        credit_limit,
        prev_age,
        ROW_NUMBER() OVER (PARTITION BY MONTH(signup_date), segment ORDER BY next_credit_limit, id) AS event_seq_2,
        SUM(prev_age) OVER (PARTITION BY MONTH(signup_date), city ORDER BY next_credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_prev_age,
        CUME_DIST() OVER (PARTITION BY DATE_TRUNC('WEEK', signup_date) ORDER BY event_seq_adjusted DESC NULLS LAST) AS event_seq_adjusted_cume_dist
    FROM valid
)
-- Multi-level aggregation
SELECT
    CASE WHEN prev_age >= 63 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_age_tier,
    PERCENTILE_APPROX(event_seq_2, 0.95) AS event_seq_2_p95,
    ROUND(AVG(prev_age), 2) AS avg_prev_age
FROM enriched
GROUP BY ROLLUP (CASE WHEN prev_age >= 63 THEN 'PREMIUM' ELSE 'STANDARD' END)
ORDER BY event_seq_2_p95 DESC;
