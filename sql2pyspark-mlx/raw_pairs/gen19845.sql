WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(amount, 0) AS amount_filled,
        COALESCE(method, 'OTHER') AS resolved_method
    FROM payments
    WHERE paid_at > DATE '2023-06-16'
      AND order_id IS NOT NULL
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        s.id,
        s.order_id,
        s.method,
        s.amount,
        s.status,
        s.resolved_method,
        b.status AS order_status,
        b.amount AS order_amount
    FROM filtered s
    INNER JOIN orders b
        ON s.order_id = b.id
),
with_dims_payments AS (
    -- Attach orders attributes
    SELECT
        e.*,
        p.shipping_fee,
        p.discount
    FROM with_dims e
    LEFT JOIN orders p
        ON e.order_id = p.id
    WHERE p.amount >= 1410
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        amount,
        shipping_fee,
        discount,
        FIRST_VALUE(order_amount) IGNORE NULLS OVER (PARTITION BY order_id ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_order_amount,
        STDDEV_SAMP(discount) OVER (PARTITION BY method) AS method_stddev_samp_discount,
        AVG(discount) OVER (PARTITION BY method) AS method_avg_discount
    FROM with_dims_payments
)
-- Join a dimension and aggregate
SELECT
    dim.channel,
    dim.status,
    COUNT(*) AS rows_total,
    MIN(a.shipping_fee) AS min_shipping_fee,
    ROUND(AVG(a.amount), 2) AS avg_amount,
    GROUPING_ID() AS level_id
FROM enriched a
LEFT JOIN orders dim ON a.order_id = dim.id
GROUP BY CUBE (dim.channel, dim.status)
ORDER BY channel DESC NULLS FIRST
