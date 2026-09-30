WITH cleaned AS (
    SELECT
        *,
        CONCAT(status, ' - ', channel) AS status_channel,
        COALESCE(channel, 'OTHER') AS resolved_channel
    FROM orders
    WHERE channel IS NOT NULL
),
enriched AS (
    SELECT
        *,
        MIN(amount) OVER w_ordered AS running_min_amount,
        COUNT(amount) OVER w_ordered AS running_count_amount,
        SUM(amount) OVER w_ordered AS running_sum_amount,
        AVG(amount) OVER w_ordered AS running_avg_amount,
        DENSE_RANK() OVER (PARTITION BY YEAR(order_date), status ORDER BY amount NULLS FIRST) AS amount_dense_rank
    FROM cleaned
    WINDOW w_ordered AS (PARTITION BY store_id, status ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_latest AS (
    SELECT
        s.id,
        s.store_id,
        s.status,
        s.channel,
        s.discount,
        s.shipping_fee,
        s.resolved_channel,
        s.running_min_amount,
        s.running_sum_amount,
        s.amount_dense_rank,
        latest.payment_method,
        latest.paid_at
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.method AS payment_method, c.paid_at
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
enriched_orders AS (
    SELECT
        *,
        SUM(amount_dense_rank) OVER (PARTITION BY channel ORDER BY amount_dense_rank, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_amount_dense_rank,
        MAX(amount_dense_rank) OVER (PARTITION BY channel ORDER BY amount_dense_rank, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_amount_dense_rank,
        MIN(amount_dense_rank) OVER (PARTITION BY channel ORDER BY amount_dense_rank, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_amount_dense_rank,
        COUNT(amount_dense_rank) OVER (PARTITION BY channel ORDER BY amount_dense_rank, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_amount_dense_rank,
        PERCENT_RANK() OVER (ORDER BY running_sum_amount NULLS LAST) AS running_sum_amount_percent_rank
    FROM with_latest
    WHERE status != 'SHIPPED'
),
with_latest_orders AS (
    SELECT
        s.*,
        top1.order_item_quantity,
        top1.order_item_unit_price
    FROM enriched_orders s
    JOIN LATERAL (
            SELECT c.quantity AS order_item_quantity, c.unit_price AS order_item_unit_price
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
metrics AS (
    SELECT
        *,
        MAX(running_sum_amount) OVER (PARTITION BY payment_method, channel ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS running_sum_amount_max_90d,
        NTILE(4) OVER (PARTITION BY channel, payment_method ORDER BY running_sum_amount, id) AS running_sum_amount_bucket
    FROM with_latest_orders
)
SELECT
    d.region,
    src.channel,
    COUNT(*) AS num_rows,
    MIN(src.running_sum_amount_percent_rank) AS min_running_sum_amount_percent_rank
FROM metrics src
LEFT JOIN stores d ON src.store_id = d.id
GROUP BY GROUPING SETS ((d.region, src.channel), (d.region), ())
ORDER BY min_running_sum_amount_percent_rank DESC NULLS LAST
