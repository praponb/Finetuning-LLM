WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        CONCAT(method, ' - ', status) AS method_status,
        COALESCE(method, 'DIRECT') AS method_clean
    FROM payments
    WHERE paid_at >= TO_TIMESTAMP('2024-02-15 19:00:00')
      AND method IS NOT NULL
),
enriched AS (
    -- Attach orders attributes
    SELECT src.*, o.discount
    FROM filtered src
    LEFT JOIN orders o
        ON src.order_id = o.id
),
enriched_payments AS (
    -- Attach orders attributes
    SELECT
        e.*,
        p.shipping_fee,
        p.amount AS order_amount
    FROM enriched e
    LEFT JOIN orders p
        ON e.order_id = p.id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        STDDEV_SAMP(shipping_fee) OVER (PARTITION BY method) AS method_stddev_samp_shipping_fee,
        DENSE_RANK() OVER w_running AS discount_dense_rank,
        AVG(shipping_fee) OVER (PARTITION BY status ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS shipping_fee_avg_90d,
        AVG(shipping_fee) OVER w_part AS shipping_fee_avg_2_prior
    FROM enriched_payments
    WINDOW w_running AS (PARTITION BY status ORDER BY discount NULLS LAST), w_part AS (PARTITION BY status ORDER BY order_amount, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        method_status,
        method_clean,
        discount,
        shipping_fee,
        order_amount,
        method_stddev_samp_shipping_fee,
        discount_dense_rank,
        shipping_fee_avg_90d,
        shipping_fee_avg_2_prior,
        ROW_NUMBER() OVER w_recent AS row_seq,
        ROW_NUMBER() OVER w_recent_2 AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount DESC, id) AS seq_no_2
    FROM scored
    WINDOW w_recent AS (ORDER BY amount, id), w_recent_2 AS (PARTITION BY order_id ORDER BY method_stddev_samp_shipping_fee DESC, id)
),
scored_payments AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(order_amount) OVER (PARTITION BY status ORDER BY discount, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS order_amount_avg_6,
        LAST_VALUE(amount) IGNORE NULLS OVER (PARTITION BY method ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_amount,
        AVG(shipping_fee_avg_2_prior) OVER (PARTITION BY method, order_id ORDER BY shipping_fee, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS shipping_fee_avg_2_prior_avg_6
    FROM with_windows
)
SELECT
    s.id,
    s.order_id,
    s.method,
    s.status,
    s.method_status,
    s.method_clean,
    s.shipping_fee,
    s.method_stddev_samp_shipping_fee,
    s.discount_dense_rank,
    CASE WHEN status RLIKE '^(SUCCESS|REFUNDED)$' THEN 'GROUP_A' WHEN status RLIKE '^(FAILED)$' THEN 'PRIORITY' ELSE 'CORE' END AS status_zone,
    (SELECT MAX(p2.amount) FROM payments p2 WHERE p2.order_id = s.order_id) AS max_payment_amount
FROM scored_payments s
