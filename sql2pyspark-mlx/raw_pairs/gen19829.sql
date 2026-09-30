WITH valid AS (
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        CASE WHEN status IN ('COMPLETED', 'SHIPPED') THEN 'Tier 1' ELSE 'Other' END AS status_group
    FROM orders
    WHERE channel != 'partner'
),
metrics AS (
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY store_id ORDER BY discount, id) AS discount_bucket,
        NTILE(10) OVER (ORDER BY shipping_fee DESC, id) AS shipping_fee_bucket,
        CORR(shipping_fee, discount) OVER (PARTITION BY store_id) AS corr_shipping_fee_discount
    FROM valid
    WHERE order_date <= '2024-10-29'
),
kept AS (
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM payments ch
            WHERE ch.order_id = s.id
              AND ch.paid_at < s.order_date
        )
),
with_last AS (
    SELECT
        s.id,
        s.store_id,
        s.order_date,
        s.amount,
        s.shipping_fee,
        s.shipping_fee_bucket,
        s.corr_shipping_fee_discount,
        latest.payment_amount
    FROM kept s
    JOIN LATERAL (
            SELECT c.amount AS payment_amount
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
not_flagged AS (
    SELECT s.*
    FROM with_last s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments x
            WHERE x.order_id = s.id
              AND x.cost BETWEEN 26 AND 94
        )
)
SELECT
    store_id,
    DATE_TRUNC('MONTH', order_date) AS activity_month,
    APPROX_PERCENTILE(shipping_fee_bucket, ARRAY(0.25, 0.5, 0.75)) AS shipping_fee_bucket_quartiles,
    ROUND(AVG(shipping_fee), 2) AS avg_shipping_fee,
    COUNT(DISTINCT id) AS distinct_ids,
    AVG(shipping_fee_bucket) AS avg_shipping_fee_bucket,
    PERCENTILE_APPROX(payment_amount, ARRAY(0.25, 0.5, 0.75)) AS payment_amount_quartiles
FROM not_flagged
GROUP BY store_id, DATE_TRUNC('MONTH', order_date)
ORDER BY avg_shipping_fee_bucket NULLS LAST;
