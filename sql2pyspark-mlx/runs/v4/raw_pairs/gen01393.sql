WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(discount AS INT) AS discount_int,
        COALESCE(channel, 'NONE') AS channel_filled
    FROM orders
    WHERE order_date >= DATE '2024-04-06'
      AND customer_id IS NOT NULL
      AND discount > 0.38
),
base_orders AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        order_date,
        status,
        channel,
        shipping_fee,
        discount_int,
        channel_filled,
        LEAST(amount, discount) AS least_amount_discount,
        COALESCE(channel, 'NONE') AS channel_filled_2
    FROM base
    WHERE order_date > DATE '2023-09-19'
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY order_date DESC, id) AS rn
        FROM base_orders
    ) ranked
    WHERE rn = 1
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        store_id,
        channel,
        COUNT(*) FILTER (WHERE status = 'PENDING') AS pending_rows,
        COUNT(*) AS event_count,
        STDDEV_SAMP(shipping_fee) AS shipping_fee_stddev,
        AVG(shipping_fee) AS avg_shipping_fee,
        AVG(shipping_fee) FILTER (WHERE channel = 'partner') AS partner_avg_shipping_fee
    FROM ranked
    GROUP BY 1, 2
    HAVING STDDEV_SAMP(shipping_fee) < 0
)
SELECT
    s.store_id,
    s.channel,
    s.pending_rows,
    s.event_count,
    s.avg_shipping_fee,
    CASE WHEN channel RLIKE '^(mobile|store)$' THEN 'PRIORITY' WHEN channel RLIKE '^(web|partner)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS channel_zone,
    (SELECT SUM(x.quantity) FROM inventory x WHERE x.store_id = s.store_id) AS sum_inventory_quantity
FROM stats s
ORDER BY store_id DESC NULLS FIRST
