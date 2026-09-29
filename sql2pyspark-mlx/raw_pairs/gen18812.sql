WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        method,
        amount,
        status,
        LOWER(status) AS status_lower
    FROM payments
    WHERE order_id IS NOT NULL
      AND paid_at >= '2024-02-01'
),
enriched AS (
    -- Attach orders attributes
    SELECT
        s.id,
        s.order_id,
        s.amount,
        s.status,
        b.amount AS order_amount,
        b.discount
    FROM filtered s
    INNER JOIN orders b
        ON s.order_id = b.id
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        a.id,
        a.order_id,
        a.amount,
        a.status,
        a.discount,
        p.shipping_fee,
        p.order_date
    FROM enriched a
    INNER JOIN orders p
        ON a.order_id <=> p.id
    WHERE p.amount >= 1500
),
enriched_payments AS (
    -- Attach orders attributes
    SELECT
        e.id,
        e.order_id,
        e.amount,
        e.status,
        e.order_date,
        o.status AS order_status,
        o.discount AS order_discount,
        o.order_date AS order_order_date
    FROM with_dims e
    LEFT JOIN orders o
        ON e.order_id <=> o.id
),
with_dims_payments AS (
    -- Attach orders attributes
    SELECT
        e.id,
        e.order_id,
        e.amount,
        e.order_date,
        e.order_status,
        e.order_order_date,
        p.channel
    FROM enriched_payments e
    LEFT JOIN orders p
        ON e.order_id <=> p.id
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM with_dims_payments a
    INNER JOIN promotions pr
        ON COALESCE(a.channel, 'web') = pr.channel
       AND a.order_date BETWEEN pr.starts_on AND pr.ends_on
)
SELECT
    id,
    order_id,
    amount,
    order_date,
    order_status,
    order_order_date,
    promo_id,
    promo_code
FROM matched
ORDER BY amount DESC
