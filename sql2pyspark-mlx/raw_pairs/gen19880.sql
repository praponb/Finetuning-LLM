WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        store_id,
        status,
        channel,
        discount,
        shipping_fee,
        discount - shipping_fee discount_minus_shipping_fee
    FROM orders
    WHERE order_date >= DATE '2023-08-23'
      AND channel IS NOT DISTINCT FROM 'store'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(discount_minus_shipping_fee) OVER (PARTITION BY channel ORDER BY shipping_fee ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_avg_discount_minus_shipping_fee,
        COUNT(discount_minus_shipping_fee) OVER (PARTITION BY channel ORDER BY shipping_fee ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_count_discount_minus_shipping_fee,
        MIN(discount_minus_shipping_fee) OVER (PARTITION BY channel ORDER BY shipping_fee ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_min_discount_minus_shipping_fee,
        FIRST_VALUE(channel) IGNORE NULLS OVER (PARTITION BY status ORDER BY shipping_fee ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_channel
    FROM valid
    WHERE store_id = 16
),
enriched AS (
    -- Attach stores attributes
    SELECT
        a.id,
        a.customer_id,
        a.store_id,
        a.status,
        a.channel,
        a.discount,
        a.shipping_fee,
        a.running_avg_discount_minus_shipping_fee,
        a.running_min_discount_minus_shipping_fee,
        a.first_known_channel,
        s.sqft
    FROM with_windows a
    LEFT JOIN stores s
        ON a.store_id = s.id
),
enriched_orders AS (
    -- Attach customers attributes
    SELECT e.*, d.segment
    FROM enriched e
    JOIN customers d
        ON e.customer_id = d.id
),
with_last AS (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.shipment_status,
        top1.shipped_date
    FROM enriched_orders s
    LEFT JOIN LATERAL (
            SELECT c.status AS shipment_status, c.shipped_date
            FROM shipments c
            WHERE c.order_id = s.id
            ORDER BY c.shipped_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    id,
    customer_id,
    store_id,
    status,
    channel,
    discount,
    shipping_fee,
    running_avg_discount_minus_shipping_fee,
    first_known_channel
FROM with_last
ORDER BY status ASC NULLS LAST
