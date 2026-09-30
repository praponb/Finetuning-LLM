WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        c.id,
        c.customer_id,
        c.coupon_codes,
        elem.product_id,
        elem.qty,
        elem.price,
        elem.promo
    FROM carts c
    LATERAL VIEW OUTER EXPLODE(c.items) e AS elem
),
lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        qty,
        price,
        promo,
        tag
    FROM exploded
    LATERAL VIEW OUTER EXPLODE(coupon_codes) li AS tag
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CONCAT_WS('-', tag, promo) AS tag_promo_key
    FROM lines
    WHERE promo IS NOT NULL
      AND price <= 150
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        qty,
        promo,
        tag,
        tag_promo_key,
        CUME_DIST() OVER (ORDER BY price DESC NULLS LAST) AS price_cume_dist,
        MAX(qty) OVER (PARTITION BY tag) AS tag_max_qty
    FROM filtered
),
metrics_carts AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY promo ORDER BY tag_max_qty DESC) AS tag_max_qty_percent_rank,
        MIN(tag_max_qty) OVER (PARTITION BY tag) AS tag_min_tag_max_qty
    FROM metrics
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        s.*,
        p.signup_date,
        p.email
    FROM metrics_carts s
    INNER JOIN customers p
        ON s.customer_id <=> p.id
)
SELECT
    s.id,
    s.customer_id,
    s.qty,
    s.promo,
    s.tag,
    s.tag_promo_key,
    s.tag_min_tag_max_qty,
    s.signup_date,
    s.email,
    CASE WHEN promo RLIKE '^(SPRING10|VIP)$' THEN 'CORE' WHEN promo RLIKE '^(BUNDLE|WELCOME5)$' THEN 'EXTENDED' ELSE 'PRIORITY' END AS promo_zone,
    (SELECT AVG(t2.balance) FROM accounts t2 WHERE t2.customer_id = s.customer_id) AS avg_account_balance
FROM with_dims s;
