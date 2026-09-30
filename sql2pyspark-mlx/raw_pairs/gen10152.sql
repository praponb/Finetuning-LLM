WITH filtered AS (
    -- Keep recent, valid rows
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
        QUARTER(order_date) AS order_date_quarter,
        ROUND(discount * 0.85, 2) AS discount_scaled
    FROM orders
    WHERE store_id IS NOT NULL
      AND order_date > DATE '2023-06-27'
),
enriched AS (
    -- Attach stores attributes
    SELECT
        a.id,
        a.store_id,
        a.channel,
        a.discount,
        a.shipping_fee,
        d.region
    FROM filtered a
    INNER JOIN stores d
        ON a.store_id = d.id
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, COALESCE(shipping_fee, 0) AS shipping_fee_filled
    FROM enriched
    WHERE store_id IS NOT NULL
      AND channel <> 'partner'
),
with_latest AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.channel,
        s.discount,
        s.shipping_fee,
        s.region,
        s.shipping_fee_filled,
        x.quantity
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.quantity
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_latest_orders AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.channel,
        s.discount,
        s.shipping_fee,
        s.region,
        s.shipping_fee_filled,
        top1.order_item_unit_price
    FROM with_latest s
    LEFT JOIN LATERAL (
            SELECT c.unit_price AS order_item_unit_price
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        channel,
        discount,
        shipping_fee,
        region,
        shipping_fee_filled,
        order_item_unit_price,
        DENSE_RANK() OVER (ORDER BY order_item_unit_price NULLS LAST) AS order_item_unit_price_dense_rank,
        NTILE(4) OVER (PARTITION BY region ORDER BY order_item_unit_price, id) AS order_item_unit_price_bucket,
        STDDEV_SAMP(discount) OVER (PARTITION BY region) AS region_stddev_samp_discount
    FROM with_latest_orders
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY shipping_fee_filled DESC, id) AS event_seq,
        NTILE(10) OVER (PARTITION BY channel ORDER BY order_item_unit_price_bucket DESC, id) AS order_item_unit_price_bucket_bucket,
        NTILE(4) OVER (ORDER BY shipping_fee DESC, id) AS shipping_fee_bucket
    FROM with_windows
)
-- Aggregate with conditional metrics
SELECT
    channel,
    region,
    CASE WHEN order_item_unit_price >= 440 THEN 'LARGE' ELSE 'SMALL' END AS order_item_unit_price_tier,
    COUNT(DISTINCT region) AS distinct_regions,
    SIZE(COLLECT_SET(channel)) AS num_channels,
    SORT_ARRAY(COLLECT_SET(channel)) AS channel_list,
    SIZE(COLLECT_SET(region)) AS num_regions
FROM scored
GROUP BY channel, region, order_item_unit_price_tier
ORDER BY region
