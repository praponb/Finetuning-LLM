WITH exploded AS (
    SELECT
        src.id,
        src.customer_id,
        src.created_at,
        src.coupon_codes,
        rec.product_id,
        rec.qty,
        rec.price,
        rec.promo
    FROM carts src
    LATERAL VIEW EXPLODE(src.items) li AS rec
),
lines AS (
    SELECT
        customer_id,
        created_at,
        qty,
        price,
        promo,
        EXPLODE(coupon_codes) AS value
    FROM exploded
),
with_windows AS (
    SELECT
        customer_id,
        qty,
        value,
        COUNT(qty) OVER (PARTITION BY customer_id, value ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) qty_count_15m,
        RANK() OVER (PARTITION BY promo ORDER BY qty DESC) qty_rank,
        AVG(qty) OVER (PARTITION BY promo) promo_avg_qty
    FROM lines
    WHERE price < 200
),
with_dims AS (
    SELECT
        s.customer_id,
        s.value,
        s.qty_count_15m,
        s.promo_avg_qty,
        c.city
    FROM with_windows s
    LEFT JOIN customers c
        ON s.customer_id = c.id
),
base AS (
    SELECT
        customer_id,
        value,
        qty_count_15m,
        promo_avg_qty,
        qty_count_15m * promo_avg_qty qty_count_15m_times_promo_avg_qty,
        ROUND(qty_count_15m, 0) qty_count_15m_rounded
    FROM with_dims
    WHERE customer_id IS NOT NULL
)
SELECT
    s.customer_id,
    s.value,
    s.promo_avg_qty,
    (SELECT MIN(t2.monthly_fee) FROM subscriptions t2 WHERE t2.customer_id = s.customer_id) AS min_subscription_monthly_fee
FROM base s
