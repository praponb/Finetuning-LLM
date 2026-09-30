WITH enriched AS (
    -- Attach customers attributes
    SELECT
        a.*,
        c.city,
        c.credit_limit
    FROM subscriptions a
    LEFT JOIN customers c
        ON a.customer_id = c.id
),
enriched_subscriptions AS (
    -- Attach customers attributes
    SELECT s.*, d.age
    FROM enriched s
    LEFT JOIN customers d
        ON s.customer_id = d.id
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, INITCAP(TRIM(plan)) AS plan_title
    FROM enriched_subscriptions
    WHERE end_date > DATE '2024-04-06'
      AND customer_id IS NOT NULL
)
SELECT
    s.id,
    s.customer_id,
    s.plan,
    s.start_date,
    s.monthly_fee,
    s.credit_limit,
    s.age,
    s.plan_title,
    CASE WHEN plan RLIKE '^(enterprise|premium)$' THEN 'EXTENDED' ELSE 'OTHER_ZONE' END AS plan_zone,
    (SELECT SUM(o2.discount) FROM orders o2 WHERE o2.customer_id = s.customer_id) AS sum_order_discount
FROM cleaned s
ORDER BY monthly_fee ASC NULLS LAST
