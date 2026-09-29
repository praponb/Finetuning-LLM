WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT(name, ' ', country) AS name_country,
        COALESCE(segment, 'OTHER') AS resolved_segment
    FROM customers
    WHERE city IS NOT NULL
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM accounts ch
            WHERE ch.customer_id = s.id
              AND ch.opened_date < s.signup_date
        )
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT(country, ' - ', email) AS country_email,
        SUBSTRING(country, 1, 4) AS country_prefix,
        COALESCE(segment, 'NONE') AS segment_filled
    FROM kept
    WHERE city IS NOT NULL
      AND credit_limit < 9900
),
with_latest AS (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.subscription_monthly_fee,
        top1.subscription_plan
    FROM cleaned s
    JOIN LATERAL (
            SELECT c.monthly_fee AS subscription_monthly_fee, c.plan AS subscription_plan
            FROM subscriptions c
            WHERE c.customer_id = s.id
            ORDER BY c.monthly_fee DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
totals AS (
    -- Grouped metrics
    SELECT
        CASE WHEN credit_limit >= 12300 THEN 'LARGE' ELSE 'SMALL' END AS credit_limit_tier,
        CASE WHEN age >= 64 THEN 'LARGE' ELSE 'SMALL' END AS age_tier,
        country,
        CORR(credit_limit, age) AS corr_credit_limit_age,
        AVG(subscription_monthly_fee) AS avg_subscription_monthly_fee,
        ROUND(SUM(age) / NULLIF(COUNT(CASE WHEN city = 'Bangkok' THEN 1 END), 0), 2) AS age_per_bangkok,
        COUNT(CASE WHEN country = 'JP' THEN 1 END) AS jp_count,
        SUM(age) FILTER (WHERE subscription_plan = 'standard') AS standard_total_age,
        MAX(credit_limit) FILTER (WHERE segment = 'SMB') AS smb_max_credit_limit,
        GROUPING(CASE WHEN credit_limit >= 12300 THEN 'LARGE' ELSE 'SMALL' END) AS gid
    FROM with_latest
    GROUP BY ROLLUP (CASE WHEN credit_limit >= 12300 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN age >= 64 THEN 'LARGE' ELSE 'SMALL' END, country)
)
SELECT
    credit_limit_tier,
    age_tier,
    corr_credit_limit_age,
    avg_subscription_monthly_fee,
    standard_total_age,
    smb_max_credit_limit,
    gid
FROM totals
ORDER BY gid DESC NULLS LAST, corr_credit_limit_age
