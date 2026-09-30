WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        COALESCE(monthly_fee, 0) AS monthly_fee_filled
    FROM subscriptions
    WHERE start_date >= DATE '2023-03-04'
      AND customer_id IS NOT NULL
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.end_date,
        e.monthly_fee_filled,
        b.city
    FROM valid e
    LEFT JOIN customers b
        ON e.customer_id <=> b.id
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY customer_id ORDER BY monthly_fee_filled NULLS LAST) AS rn
    FROM with_dims
    QUALIFY rn = 1
),
with_dims_subscriptions AS (
    -- Attach customers attributes
    SELECT
        a.*,
        d.city AS customer_city,
        d.name
    FROM ranked a
    LEFT JOIN customers d
        ON a.customer_id = d.id
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM with_dims_subscriptions a
    JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.end_date >= sub.start_date
       AND a.end_date <= DATE_ADD(sub.end_date, 10)
)
-- Join a dimension and aggregate
SELECT
    c.segment,
    c.country,
    e.plan,
    COUNT(*) AS cnt,
    MIN(e.subscription_id) AS min_subscription_id,
    GROUPING_ID() AS gid
FROM attributed e
JOIN customers c ON e.customer_id = c.id
GROUP BY CUBE (c.segment, c.country, e.plan)
ORDER BY cnt DESC
