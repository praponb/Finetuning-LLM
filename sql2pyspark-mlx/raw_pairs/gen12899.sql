WITH with_last AS (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.subscription_plan
    FROM customers s
    LEFT JOIN LATERAL (
            SELECT c.plan AS subscription_plan
            FROM subscriptions c
            WHERE c.customer_id = s.id
            ORDER BY c.monthly_fee DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(is_active AS INT) AS is_active_flag,
        ADD_MONTHS(signup_date, 12) AS signup_date_plus_months,
        COALESCE(segment, 'OTHER') AS resolved_segment
    FROM with_last
    WHERE city = 'Berlin'
      AND city IS DISTINCT FROM 'Berlin'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(age) OVER (PARTITION BY country ORDER BY signup_date_plus_months ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_age,
        SUM(age) OVER (PARTITION BY country ORDER BY signup_date_plus_months ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_age,
        MAX(age) OVER (PARTITION BY country ORDER BY signup_date_plus_months ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_age,
        MIN(age) OVER (PARTITION BY country ORDER BY signup_date_plus_months ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_age
    FROM cleaned
    WHERE is_active_flag BETWEEN 41 AND 73
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        credit_limit,
        subscription_plan,
        is_active_flag,
        signup_date_plus_months,
        resolved_segment,
        last3_count_age,
        last3_sum_age,
        last3_min_age,
        CASE WHEN city = 'London' THEN 1 ELSE 0 END AS is_london,
        COALESCE(subscription_plan, 'NONE') AS subscription_plan_clean
    FROM metrics
    WHERE segment = 'Corporate'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(last3_min_age) OVER (PARTITION BY subscription_plan ORDER BY is_active_flag ASC, id ASC) AS prev_last3_min_age,
        LEAD(credit_limit) OVER (PARTITION BY subscription_plan ORDER BY is_active_flag ASC, id ASC) AS next_credit_limit,
        ROW_NUMBER() OVER (PARTITION BY subscription_plan ORDER BY is_active_flag ASC, id ASC) AS row_seq,
        PERCENT_RANK() OVER (PARTITION BY city, segment ORDER BY credit_limit DESC NULLS LAST) AS credit_limit_percent_rank
    FROM valid
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.customer_id = s.id
              AND ch.order_date >= s.signup_date
        )
)
-- Multi-level aggregation
SELECT
    CASE WHEN row_seq >= 7 THEN 'LARGE' ELSE 'SMALL' END AS row_seq_tier,
    DATE_TRUNC('MONTH', signup_date) AS activity_month,
    CASE WHEN last3_sum_age >= 287 THEN 'LARGE' ELSE 'SMALL' END AS last3_sum_age_tier,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    COUNT_IF(segment = 'SMB') AS smb_events,
    COUNT(*) FILTER (WHERE country = 'DE') AS de_rows,
    AVG(last3_min_age) AS avg_last3_min_age,
    ROUND(SUM(last3_sum_age) / NULLIF(COUNT(CASE WHEN subscription_plan = 'standard' THEN 1 END), 0), 2) AS last3_sum_age_per_standard
FROM kept
GROUP BY GROUPING SETS ((CASE WHEN row_seq >= 7 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('MONTH', signup_date), CASE WHEN last3_sum_age >= 287 THEN 'LARGE' ELSE 'SMALL' END), (CASE WHEN row_seq >= 7 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('MONTH', signup_date)), (CASE WHEN row_seq >= 7 THEN 'LARGE' ELSE 'SMALL' END), ());
