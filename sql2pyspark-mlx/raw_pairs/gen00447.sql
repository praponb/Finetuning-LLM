WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, UPPER(method) AS method_upper
    FROM payments
    WHERE paid_at > TO_TIMESTAMP('2023-09-23 16:00:00')
      AND method IS DISTINCT FROM 'promptpay'
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        src.*,
        o.shipping_fee,
        o.discount,
        o.order_date
    FROM valid src
    LEFT JOIN orders o
        ON src.order_id = o.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        shipping_fee,
        discount,
        LAST_VALUE(amount) IGNORE NULLS OVER w_running AS last_known_amount,
        FIRST_VALUE(discount, TRUE) OVER (PARTITION BY status, order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_discount,
        DENSE_RANK() OVER (PARTITION BY method ORDER BY discount NULLS FIRST) AS discount_dense_rank,
        MAX(shipping_fee) OVER (PARTITION BY method) AS method_max_shipping_fee,
        LAG(discount, 1) OVER w_ordered AS prev_discount
    FROM with_dims
    WINDOW w_running AS (PARTITION BY status ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY DATE_TRUNC('MONTH', paid_at), status ORDER BY order_date, id)
),
enriched AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.method,
        src.amount,
        src.paid_at,
        src.status,
        src.shipping_fee,
        src.discount,
        src.last_known_amount,
        src.first_known_discount,
        src.prev_discount,
        b.channel,
        b.status AS order_status,
        b.amount AS order_amount
    FROM with_windows src
    LEFT JOIN orders b
        ON src.order_id = b.id
),
enriched_payments AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        amount,
        status,
        shipping_fee,
        last_known_amount,
        channel,
        order_status,
        order_amount,
        ROW_NUMBER() OVER (PARTITION BY status, order_id ORDER BY first_known_discount DESC, id) AS row_seq,
        AVG(order_amount) OVER (PARTITION BY order_id ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS order_amount_avg_90d,
        ROUND(last_known_amount / SUM(last_known_amount) OVER (PARTITION BY method), 4) AS last_known_amount_share,
        CUME_DIST() OVER (PARTITION BY status ORDER BY shipping_fee) AS shipping_fee_cume_dist,
        AVG(shipping_fee) OVER (PARTITION BY method, order_id ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS shipping_fee_avg_90d
    FROM enriched
    WHERE shipping_fee <= 19
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        amount,
        shipping_fee,
        last_known_amount,
        channel,
        order_status,
        order_amount,
        row_seq,
        order_amount_avg_90d,
        last_known_amount_share,
        shipping_fee_cume_dist,
        shipping_fee_avg_90d,
        GREATEST(last_known_amount, last_known_amount_share) AS greatest_last_known_amount_last_known_amount_share,
        CONCAT_WS('-', status, order_status) AS status_order_status_key
    FROM enriched_payments
    WHERE channel IS NOT NULL
),
wide AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            channel,
            order_status,
            last_known_amount
        FROM filtered
    ) src
    PIVOT (
        AVG(last_known_amount) AS value_sum
        FOR order_status IN ('CANCELLED', 'COMPLETED')
    )
)
SELECT
    channel,
    CANCELLED,
    COMPLETED,
    CASE WHEN channel RLIKE '^(mobile|partner)$' THEN 'OTHER_ZONE' WHEN channel RLIKE '^(web|store)$' THEN 'EXTENDED' ELSE 'GROUP_A' END AS channel_zone
FROM wide
