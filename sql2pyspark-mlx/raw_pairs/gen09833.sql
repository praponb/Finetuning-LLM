WITH enriched AS (
    SELECT
        id,
        store_id,
        status,
        channel,
        amount,
        shipping_fee,
        SUM(discount) OVER w_ordered cumulative_discount,
        CORR(amount, shipping_fee) OVER (PARTITION BY store_id) corr_amount_shipping_fee,
        AVG(shipping_fee) OVER (PARTITION BY customer_id ORDER BY amount ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) shipping_fee_avg_5_prior,
        NTILE(4) OVER (PARTITION BY customer_id ORDER BY discount DESC, id ASC) discount_bucket,
        SUM(shipping_fee) OVER (ORDER BY amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_shipping_fee
    FROM orders
    WHERE channel IN ('web', 'partner', 'store')
    WINDOW w_ordered AS (ORDER BY shipping_fee ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
not_flagged AS (
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM payments c
            WHERE c.order_id = s.id
              AND c.paid_at < '2024-05-13'
        )
),
not_flagged_orders AS (
    SELECT s.*
    FROM not_flagged s
    WHERE EXISTS (
            SELECT 1
            FROM payments x
            WHERE x.order_id = s.id
              AND x.paid_at >= '2024-08-01'
        )
),
not_flagged_orders_2 AS (
    SELECT s.*
    FROM not_flagged_orders s
    WHERE EXISTS (
            SELECT 1
            FROM shipments ch
            WHERE ch.order_id = s.id
              AND ch.carrier IN ('UPS', 'Flash')
        )
),
with_windows AS (
    SELECT
        id,
        status,
        channel,
        amount,
        shipping_fee,
        cumulative_discount,
        corr_amount_shipping_fee,
        discount_bucket,
        cumulative_shipping_fee,
        LAG(shipping_fee, 1) OVER (PARTITION BY store_id ORDER BY cumulative_discount ASC, id ASC) prev_shipping_fee,
        LEAD(cumulative_discount, 1) OVER (ORDER BY corr_amount_shipping_fee ASC, id ASC) next_cumulative_discount,
        ROW_NUMBER() OVER w_grp seq_no
    FROM not_flagged_orders_2
    WINDOW w_grp AS (PARTITION BY status ORDER BY cumulative_discount ASC, id ASC)
)
SELECT
    id,
    status,
    channel,
    cumulative_discount,
    prev_shipping_fee,
    next_cumulative_discount,
    seq_no,
    CASE WHEN channel RLIKE '^(partner|store)$' THEN 'CORE' WHEN channel RLIKE '^(web|mobile)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS channel_zone
FROM with_windows
