WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        tag
    FROM carts
    LATERAL VIEW EXPLODE(coupon_codes) e AS tag
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_FORMAT(created_at, 'yyyy-MM') AS created_at_label
    FROM exploded
    WHERE created_at > DATE '2025-03-01'
      AND tag IS DISTINCT FROM 'VIP'
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.tag,
        d.segment,
        d.name
    FROM cleaned e
    LEFT JOIN customers d
        ON e.customer_id <=> d.id
    WHERE d.is_active
),
enriched AS (
    -- Attach customers attributes
    SELECT e.*, d.signup_date
    FROM with_dims e
    LEFT JOIN customers d
        ON e.customer_id = d.id
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('QUARTER', signup_date) AS quarter_start,
        customer_id,
        SORT_ARRAY(COLLECT_SET(segment)) AS segment_list,
        COUNT_IF(segment = 'Corporate') AS corporate_events,
        COUNT(CASE WHEN segment = 'Corporate' THEN 1 END) AS corporate_count,
        SIZE(COLLECT_SET(tag)) AS num_tags,
        GROUPING_ID() AS gid
    FROM enriched
    GROUP BY CUBE (DATE_TRUNC('QUARTER', signup_date), customer_id)
)
SELECT
    s.quarter_start,
    s.customer_id,
    s.segment_list,
    s.corporate_events,
    s.num_tags,
    s.gid,
    (SELECT MIN(s2.monthly_fee) FROM subscriptions s2 WHERE s2.customer_id = s.customer_id) AS min_subscription_monthly_fee
FROM grouped s
ORDER BY min_subscription_monthly_fee DESC NULLS LAST, gid DESC;
