WITH eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM customers s
    WHERE EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.customer_id = s.id
              AND ch.amount < 1050
        )
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LENGTH(name) AS name_length,
        COALESCE(segment, 'UNKNOWN') AS resolved_segment
    FROM eligible
    WHERE city = 'London'
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions x
            WHERE x.customer_id = s.id
              AND x.started_at < s.signup_date
        )
),
latest_match AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.name,
        s.email,
        s.country,
        s.city,
        s.segment,
        s.signup_date,
        s.credit_limit,
        s.resolved_segment,
        top1.order_discount
    FROM not_flagged s
    JOIN LATERAL (
            SELECT c.discount AS order_discount
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.order_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        name,
        segment,
        credit_limit,
        order_discount,
        NTILE(3) OVER (PARTITION BY segment ORDER BY order_discount DESC, id) AS order_discount_bucket,
        AVG(order_discount) OVER (PARTITION BY country ORDER BY credit_limit, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS order_discount_avg_6,
        SUM(order_discount) OVER (PARTITION BY city ORDER BY order_discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_order_discount,
        AVG(order_discount) OVER (PARTITION BY country) AS country_avg_order_discount,
        AVG(order_discount) OVER (PARTITION BY country ORDER BY credit_limit, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS order_discount_avg_6_2
    FROM latest_match
    WHERE city = 'Tokyo'
)
SELECT
    id,
    name,
    credit_limit,
    order_discount_avg_6,
    running_order_discount,
    country_avg_order_discount,
    order_discount_avg_6_2,
    CASE WHEN name RLIKE '^(Alice Wong|FRANK MILLER)$' THEN 'GROUP_A' WHEN name RLIKE '^(bob smith|  Carol Diaz)$' THEN 'EXTENDED' ELSE 'CORE' END AS name_zone
FROM enriched
ORDER BY country_avg_order_discount NULLS FIRST, name_zone;
