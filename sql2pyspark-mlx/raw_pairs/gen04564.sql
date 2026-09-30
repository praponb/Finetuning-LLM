WITH valid AS (
    SELECT *, TRIM(plan) AS clean_plan
    FROM subscriptions
    WHERE customer_id IS NOT NULL
      AND customer_id IS NOT NULL
),
scored AS (
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY plan ORDER BY monthly_fee, id) AS monthly_fee_bucket,
        ROW_NUMBER() OVER (PARTITION BY status, plan ORDER BY end_date DESC, id) AS seq_no
    FROM valid
),
joined AS (
    SELECT
        e.*,
        b.age,
        b.name,
        b.email
    FROM scored e
    LEFT JOIN customers b
        ON e.customer_id = b.id
),
attributed AS (
    SELECT
        s.*,
        sub.id AS subscription_id,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM joined s
    JOIN subscriptions sub
        ON s.customer_id = sub.customer_id
       AND s.start_date BETWEEN sub.start_date AND sub.end_date
)
SELECT
    c.city,
    c.country,
    COUNT(*) AS cnt,
    MIN(s.subscription_id) AS min_subscription_id,
    MIN(s.monthly_fee_bucket) AS min_monthly_fee_bucket,
    GROUPING_ID() AS level_id
FROM attributed s
JOIN customers c ON s.customer_id = c.id
GROUP BY CUBE (c.city, c.country)
