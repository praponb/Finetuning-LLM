WITH matched_keys AS (
    -- Set difference on keys
    SELECT id, order_id
    FROM shipments
    EXCEPT DISTINCT
    SELECT id, order_id
    FROM shipments
    WHERE shipped_date >= '2025-03-30'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM shipments s
    INNER JOIN matched_keys k ON s.id = k.id AND s.order_id = k.order_id
),
enriched AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.shipped_date,
        src.delivered_date,
        src.status,
        o.shipping_fee
    FROM verified src
    LEFT JOIN orders o
        ON src.order_id <=> o.id
    WHERE o.amount <= 480
),
enriched_shipments AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        delivered_date,
        shipping_fee,
        PERCENT_RANK() OVER (ORDER BY shipping_fee DESC NULLS LAST) AS shipping_fee_percent_rank,
        STDDEV_SAMP(shipping_fee) OVER (PARTITION BY status, order_id) AS status_stddev_samp_shipping_fee,
        CUME_DIST() OVER (PARTITION BY order_id ORDER BY shipping_fee NULLS LAST) AS shipping_fee_cume_dist,
        MAX(shipping_fee) OVER (PARTITION BY status, order_id) AS status_max_shipping_fee,
        SUM(shipping_fee) OVER (PARTITION BY status, order_id) AS status_sum_shipping_fee
    FROM enriched
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.delivered_date,
        src.status_max_shipping_fee,
        p.status,
        p.shipping_fee AS order_shipping_fee
    FROM enriched_shipments src
    LEFT JOIN orders p
        ON src.order_id = p.id
)
SELECT
    id,
    status_max_shipping_fee,
    status,
    order_shipping_fee
FROM with_dims
ORDER BY status_max_shipping_fee DESC NULLS LAST;
