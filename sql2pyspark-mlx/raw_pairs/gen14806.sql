WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        method,
        amount,
        status,
        TO_DATE(paid_at) paid_at_date,
        CASE WHEN status = 'REFUNDED' THEN 1 ELSE 0 END is_refunded
    FROM payments
    WHERE order_id IS NOT NULL
)
-- Ranking and gap features
SELECT
    id,
    order_id,
    method,
    paid_at_date,
    is_refunded,
    AVG(amount) OVER (PARTITION BY status ORDER BY amount, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) amount_avg_5_prior,
    CUME_DIST() OVER (PARTITION BY order_id ORDER BY is_refunded DESC NULLS LAST) is_refunded_cume_dist,
    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY paid_at_date DESC, id) event_seq
FROM base
WHERE method NOT IN ('cash', 'promptpay');
