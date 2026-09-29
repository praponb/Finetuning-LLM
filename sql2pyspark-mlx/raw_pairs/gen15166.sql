WITH with_last AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.name,
        s.country,
        s.segment,
        s.signup_date,
        s.is_active,
        s.age,
        top1.order_status,
        top1.order_channel
    FROM customers s
    LEFT JOIN LATERAL (
            SELECT c.status AS order_status, c.channel AS order_channel
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.shipping_fee DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_latest AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.signup_date,
        s.is_active,
        s.age,
        s.order_channel,
        latest.channel,
        latest.order_shipping_fee
    FROM with_last s
    LEFT JOIN LATERAL (
            SELECT c.channel, c.shipping_fee AS order_shipping_fee
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.discount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE EXISTS (
            SELECT 1
            FROM subscriptions x
            WHERE x.customer_id = s.id
              AND YEAR(x.start_date) = 2025
        )
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        signup_date,
        is_active,
        channel,
        order_shipping_fee,
        CUME_DIST() OVER (PARTITION BY channel ORDER BY order_shipping_fee NULLS FIRST) AS order_shipping_fee_cume_dist,
        FIRST_VALUE(channel, TRUE) OVER (ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_channel
    FROM kept
    WHERE channel IN ('mobile', 'store', 'partner')
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM metrics s
    LEFT JOIN promotions pr
        ON s.channel <=> pr.channel
       AND s.signup_date >= pr.starts_on
       AND s.signup_date <= DATE_ADD(pr.ends_on, 10)
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM attributed e
    LEFT JOIN promotions pr
        ON COALESCE(e.channel, 'store') = pr.channel
       AND e.signup_date >= pr.starts_on
       AND e.signup_date <= DATE_ADD(pr.ends_on, 7)
),
with_latest_customers AS (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.country
    FROM matched s
    LEFT JOIN LATERAL (
            SELECT c.country
            FROM sessions c
            WHERE c.customer_id = s.id
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    id,
    signup_date,
    is_active,
    channel,
    order_shipping_fee,
    first_known_channel,
    promo_id,
    promo_code,
    promo_id_2,
    CASE WHEN promo_code RLIKE '^(SPRING10|VIP)$' THEN 'GROUP_B' ELSE 'CORE' END AS promo_code_zone
FROM with_latest_customers
ORDER BY channel NULLS FIRST, promo_code_zone;
