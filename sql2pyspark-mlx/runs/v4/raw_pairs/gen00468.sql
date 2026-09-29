WITH filtered AS (
    -- Filter the raw rows and derive helper columns
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
        COALESCE(channel, 'Unknown') AS channel_filled,
        CASE WHEN discount >= 0.33 THEN 'High' WHEN discount >= 0.15 THEN 'Medium' ELSE 'Low' END AS discount_band
    FROM orders
    WHERE store_id IS NOT NULL
),
with_latest AS (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.shipment_delivered_date,
        x.shipment_carrier
    FROM filtered s
    LEFT JOIN LATERAL (
            SELECT c.delivered_date AS shipment_delivered_date, c.carrier AS shipment_carrier
            FROM shipments c
            WHERE c.order_id = s.id
            ORDER BY c.shipped_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
joined AS (
    -- Attach stores attributes
    SELECT s.*, b.sqft
    FROM with_latest s
    LEFT JOIN stores b
        ON s.store_id = b.id
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount NULLS FIRST, id) AS dedup_rank
        FROM joined
    ) t
    WHERE dedup_rank = 1
)
SELECT
    id,
    store_id,
    order_date,
    status,
    channel,
    amount,
    discount,
    discount_band,
    shipment_delivered_date,
    CASE WHEN channel RLIKE '^(mobile|web)$' THEN 'PRIORITY' ELSE 'OTHER_ZONE' END AS channel_zone
FROM deduped;
