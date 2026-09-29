WITH ranked AS (
    SELECT *
    FROM payments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY method ORDER BY paid_at, id) = 1
),
metrics AS (
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        LAST_VALUE(method) IGNORE NULLS OVER (PARTITION BY order_id, method ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_method,
        SUM(amount) OVER w_hist AS running_amount,
        AVG(amount) OVER w_hist_2 AS amount_avg_6_prior,
        MIN(amount) OVER (PARTITION BY order_id) AS order_id_min_amount,
        COUNT(amount) OVER (PARTITION BY order_id, method ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS amount_count_30d
    FROM ranked
    WINDOW w_hist AS (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist_2 AS (ORDER BY paid_at, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
),
with_dims AS (
    SELECT
        e.*,
        b.shipping_fee,
        b.channel,
        b.discount
    FROM metrics e
    LEFT JOIN orders b
        ON e.order_id = b.id
),
scored AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY amount DESC, id) AS seq_no,
        LAST_VALUE(order_id_min_amount, TRUE) OVER (PARTITION BY order_id ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_order_id_min_amount,
        LEAD(running_amount, 1) OVER (PARTITION BY channel ORDER BY order_id_min_amount, id) AS next_running_amount
    FROM with_dims
)
SELECT
    id,
    order_id,
    method,
    amount,
    status,
    last_known_method,
    running_amount,
    amount_avg_6_prior,
    channel,
    CASE WHEN channel RLIKE '^(partner|mobile)$' THEN 'GROUP_B' WHEN channel RLIKE '^(web|store)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS channel_zone
FROM scored
ORDER BY channel_zone DESC;
