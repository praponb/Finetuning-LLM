WITH scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(amount) OVER w_ordered AS next_amount,
        LAG(amount) OVER w_ordered AS prev_amount,
        ROW_NUMBER() OVER w_ordered AS row_seq
    FROM payments
    WHERE method <> 'card'
    WINDOW w_ordered AS (PARTITION BY order_id, method ORDER BY amount ASC, id ASC)
),
enriched AS (
    -- Attach orders attributes
    SELECT
        e.*,
        p.amount AS order_amount,
        p.channel,
        p.shipping_fee
    FROM scored e
    LEFT JOIN orders p
        ON e.order_id = p.id
),
enriched_payments AS (
    -- Attach orders attributes
    SELECT
        s.id,
        s.order_id,
        s.method,
        s.paid_at,
        s.next_amount,
        s.prev_amount,
        s.order_amount,
        s.channel,
        p.shipping_fee AS order_shipping_fee,
        p.discount,
        p.status AS order_status
    FROM enriched s
    LEFT JOIN orders p
        ON s.order_id = p.id
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(discount * 100, 2) AS discount_scaled
    FROM enriched_payments
    WHERE method IS NOT NULL
      AND channel IS DISTINCT FROM 'mobile'
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        s.id,
        s.order_id,
        s.method,
        s.prev_amount,
        s.channel,
        s.order_shipping_fee,
        s.discount,
        s.discount_scaled,
        p.channel AS order_channel,
        p.amount
    FROM cleaned s
    JOIN orders p
        ON s.order_id = p.id
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(order_shipping_fee, 0) AS order_shipping_fee_filled,
        COALESCE(channel, 'NONE') AS resolved_channel
    FROM with_dims
    WHERE order_id IS NOT NULL
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN order_shipping_fee_filled >= 85 THEN 'HIGH' ELSE 'LOW' END AS order_shipping_fee_filled_tier,
    CASE WHEN order_shipping_fee >= 41 THEN 'HIGH' ELSE 'LOW' END AS order_shipping_fee_tier,
    ROUND(AVG(discount), 2) AS avg_discount,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) FILTER (WHERE channel = 'web') AS web_rows,
    COUNT(*) AS num_rows,
    APPROX_PERCENTILE(prev_amount, 0.95) AS prev_amount_p95,
    AVG(discount_scaled) AS avg_discount_scaled
FROM valid
GROUP BY CASE WHEN order_shipping_fee_filled >= 85 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN order_shipping_fee >= 41 THEN 'HIGH' ELSE 'LOW' END
ORDER BY prev_amount_p95 DESC NULLS FIRST;
