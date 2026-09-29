WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        INITCAP(TRIM(plan)) AS plan_title,
        DATE_FORMAT(start_date, 'MMM yyyy') AS start_date_label
    FROM subscriptions
    WHERE start_date >= DATE '2023-10-23'
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY plan ORDER BY monthly_fee DESC, id ASC) = 1
),
joined AS (
    -- Attach customers attributes
    SELECT
        a.*,
        d.city,
        d.segment
    FROM ranked a
    LEFT JOIN customers d
        ON a.customer_id = d.id
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        sub.id AS subscription_id,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM joined a
    LEFT JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.start_date >= sub.start_date
       AND a.start_date < sub.end_date + INTERVAL 7 DAYS
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        sub.id AS subscription_id_2,
        sub.plan AS subscription_plan_2,
        sub.monthly_fee AS subscription_monthly_fee_2
    FROM with_window src
    LEFT JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.end_date BETWEEN sub.start_date AND sub.end_date
),
joined_subscriptions AS (
    -- Attach customers attributes
    SELECT
        e.*,
        b.signup_date,
        b.age,
        b.country
    FROM attributed e
    LEFT JOIN customers b
        ON e.customer_id = b.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(subscription_id, 1) OVER (PARTITION BY city ORDER BY subscription_monthly_fee_2 ASC, id ASC, subscription_id ASC, subscription_id_2 ASC) AS prev_subscription_id,
        LAG(subscription_id, 1) OVER (PARTITION BY plan ORDER BY subscription_id ASC, id ASC, subscription_id ASC, subscription_id_2 ASC) AS prev_subscription_id_2,
        LAG(subscription_monthly_fee_2, 1) OVER (PARTITION BY subscription_plan ORDER BY subscription_monthly_fee_2 ASC, id ASC, subscription_id ASC, subscription_id_2 ASC) AS prev_subscription_monthly_fee_2
    FROM joined_subscriptions
)
-- Multi-level aggregation
SELECT
    CASE WHEN age >= 73 THEN 'LARGE' ELSE 'SMALL' END AS age_tier,
    segment,
    COUNT(*) AS row_count,
    COUNT(*) AS cnt,
    ROUND(SUM(age) / NULLIF(COUNT(CASE WHEN city = 'Singapore' THEN 1 END), 0), 2) AS age_per_singapore,
    COUNT(DISTINCT city) AS distinct_cities,
    COUNT(CASE WHEN plan = 'premium' THEN 1 END) AS premium_count
FROM scored
GROUP BY CUBE (CASE WHEN age >= 73 THEN 'LARGE' ELSE 'SMALL' END, segment)
HAVING COUNT(*) > 1
