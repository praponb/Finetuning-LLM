WITH with_windows AS (
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
        STDDEV_SAMP(shipping_fee) OVER (PARTITION BY channel) AS channel_stddev_samp_shipping_fee,
        LEAD(shipping_fee, 1, 0) OVER (PARTITION BY customer_id ORDER BY discount, id) AS next_shipping_fee,
        ROW_NUMBER() OVER (PARTITION BY status, customer_id ORDER BY amount DESC, id) AS row_seq
    FROM orders
),
base AS (
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
        channel_stddev_samp_shipping_fee,
        row_seq,
        COALESCE(row_seq, 0) AS row_seq_filled
    FROM with_windows
    WHERE customer_id IS NOT NULL
),
with_latest AS (
    SELECT s.*, latest.payment_paid_at
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.paid_at AS payment_paid_at
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.paid_at DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
summary AS (
    SELECT
        CASE WHEN channel_stddev_samp_shipping_fee >= 41 THEN 'PREMIUM' ELSE 'STANDARD' END AS channel_stddev_samp_shipping_fee_tier,
        ROUND(AVG(channel_stddev_samp_shipping_fee), 2) AS avg_channel_stddev_samp_shipping_fee,
        COUNT(CASE WHEN status = 'PENDING' THEN 1 END) AS pending_count,
        COUNT(CASE WHEN status = 'RETURNED' THEN 1 END) AS returned_count,
        PERCENTILE_APPROX(row_seq, 0.9) AS row_seq_p90,
        APPROX_PERCENTILE(row_seq, 0.95) AS row_seq_p95,
        GROUPING(CASE WHEN channel_stddev_samp_shipping_fee >= 41 THEN 'PREMIUM' ELSE 'STANDARD' END) AS grouping_id
    FROM with_latest
    GROUP BY CUBE (CASE WHEN channel_stddev_samp_shipping_fee >= 41 THEN 'PREMIUM' ELSE 'STANDARD' END)
)
SELECT
    channel_stddev_samp_shipping_fee_tier,
    avg_channel_stddev_samp_shipping_fee,
    returned_count,
    row_seq_p90
FROM summary
ORDER BY returned_count, row_seq_p90;
