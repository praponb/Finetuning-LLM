WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        discount * amount AS discount_times_amount,
        ROUND(discount, 2) AS discount_rounded
    FROM orders
    WHERE channel IS NOT DISTINCT FROM 'web'
      AND customer_id IS NULL
),
latest_match AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.order_item_quantity
    FROM base s
    JOIN LATERAL (
            SELECT c.quantity AS order_item_quantity
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        store_id,
        status,
        channel,
        shipping_fee,
        discount_times_amount,
        LAG(discount, 1) OVER (PARTITION BY MONTH(order_date), status ORDER BY order_item_quantity, id) AS prev_discount,
        ROW_NUMBER() OVER (ORDER BY order_date, id) AS seq_no,
        LAG(order_item_quantity, 1) OVER (PARTITION BY customer_id, channel ORDER BY discount_rounded, id) AS prev_order_item_quantity,
        PERCENT_RANK() OVER (ORDER BY discount_times_amount DESC NULLS FIRST) AS discount_times_amount_percent_rank,
        LAG(order_item_quantity, 1) OVER (PARTITION BY channel ORDER BY discount_rounded, id) AS prev_order_item_quantity_2
    FROM latest_match
),
base_orders AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        prev_order_item_quantity + prev_discount AS prev_order_item_quantity_plus_prev_discount,
        COALESCE(channel, 'OTHER') AS channel_filled
    FROM enriched
    WHERE prev_order_item_quantity_2 != 7
)
-- Aggregate with conditional metrics
SELECT
    channel,
    CASE WHEN seq_no >= 7 THEN 'HIGH' ELSE 'LOW' END AS seq_no_tier,
    ROUND(SUM(prev_order_item_quantity_plus_prev_discount) / NULLIF(COUNT(CASE WHEN status = 'SHIPPED' THEN 1 END), 0), 2) AS prev_order_item_quantity_plus_prev_discount_per_shipped,
    SORT_ARRAY(COLLECT_SET(channel)) AS channel_list,
    COUNT_IF(channel = 'mobile') AS mobile_events,
    MAX(shipping_fee) AS max_shipping_fee,
    COUNT(DISTINCT id) AS distinct_ids,
    SUM(discount_times_amount_percent_rank) FILTER (WHERE status = 'COMPLETED') AS completed_total_discount_times_amount_percent_rank
FROM base_orders
GROUP BY channel, CASE WHEN seq_no >= 7 THEN 'HIGH' ELSE 'LOW' END
