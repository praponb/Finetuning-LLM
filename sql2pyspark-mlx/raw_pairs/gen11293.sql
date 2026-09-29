WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        SPLIT(LOWER(email), '@')[0] AS email_user,
        COALESCE(segment, 'DIRECT') AS segment_clean
    FROM customers
    WHERE signup_date > DATE '2023-03-06'
      AND name LIKE 'Mi%'
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY signup_date NULLS LAST, id) AS latest_rank
        FROM cleaned
    ) t
    WHERE latest_rank = 1
),
deduped_customers AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM deduped
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY age DESC, id) = 1
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(latest_rank / age, 2) AS latest_rank_per_age,
        age * latest_rank AS age_times_latest_rank
    FROM deduped_customers
    WHERE segment IS NOT NULL
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.started_at <= s.signup_date
        )
),
cleaned_customers AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        name,
        email,
        country,
        segment,
        credit_limit,
        email_user,
        segment_clean,
        age_times_latest_rank,
        age_times_latest_rank + credit_limit AS age_times_latest_rank_plus_credit_limit
    FROM kept
    WHERE city IS NOT NULL
      AND age_times_latest_rank >= 64
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned_customers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions x
            WHERE x.customer_id = s.id
              AND x.country IN ('SG', 'TH', 'UK')
        )
)
-- Multi-level aggregation
SELECT
    country,
    segment,
    COUNT(*) AS cnt,
    ROUND(AVG(credit_limit), 2) AS avg_credit_limit,
    PERCENTILE_APPROX(age_times_latest_rank, 0.95) AS age_times_latest_rank_p95,
    ROUND(AVG(age_times_latest_rank), 2) AS avg_age_times_latest_rank,
    RANK() OVER (PARTITION BY country ORDER BY ROUND(AVG(age_times_latest_rank), 2) DESC) AS avg_age_times_latest_rank_rank
FROM not_flagged
GROUP BY country, segment
ORDER BY avg_credit_limit DESC NULLS FIRST
