WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        name,
        email,
        city,
        segment,
        credit_limit,
        age,
        CAST(is_active AS INT) AS is_active_flag
    FROM customers
    WHERE signup_date >= TO_DATE('2024-03-24')
      AND city IS NOT NULL
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        name,
        email,
        segment,
        credit_limit,
        age,
        is_active_flag,
        LEAST(is_active_flag, age) AS least_is_active_flag_age,
        COALESCE(segment, 'UNKNOWN') AS resolved_segment
    FROM base
    WHERE name LIKE 'Emm%'
),
with_last AS (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.monthly_fee,
        top1.subscription_plan
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.monthly_fee, c.plan AS subscription_plan
            FROM subscriptions c
            WHERE c.customer_id = s.id
            ORDER BY c.start_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions x
            WHERE x.customer_id = s.id
              AND x.status = 'PAUSED'
        )
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(credit_limit) OVER (PARTITION BY subscription_plan ORDER BY age, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS credit_limit_avg_2_prior,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY monthly_fee DESC, id) AS seq_no
    FROM not_flagged
)
-- Window metrics over several different partitions
SELECT
    *,
    FIRST_VALUE(age) IGNORE NULLS OVER (ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_age,
    LEAD(credit_limit_avg_2_prior, 2) OVER (ORDER BY least_is_active_flag_age, id) AS next_credit_limit_avg_2_prior,
    SUM(monthly_fee) OVER (ORDER BY is_active_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_monthly_fee
FROM scored
