WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        order_date,
        status,
        amount,
        discount,
        shipping_fee,
        ADD_MONTHS(order_date, 6) order_date_plus_months,
        COALESCE(channel, 'UNKNOWN') resolved_channel
    FROM orders
    WHERE customer_id IS NULL
      AND order_date > TO_DATE('2023-06-24')
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        order_date,
        amount,
        shipping_fee,
        resolved_channel
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY order_date_plus_months DESC, id) = 1
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_date,
        amount,
        shipping_fee,
        resolved_channel,
        ROUND(shipping_fee, 2) shipping_fee_rounded
    FROM ranked
    WHERE order_date > TO_DATE('2023-07-14')
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        resolved_channel,
        AVG(shipping_fee) OVER (ORDER BY amount, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) shipping_fee_avg_3,
        NTILE(5) OVER (ORDER BY shipping_fee DESC, id) shipping_fee_bucket,
        RANK() OVER (ORDER BY amount DESC) amount_rank
    FROM valid
    WHERE order_date < '2024-08-01'
),
with_latest AS (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.carrier,
        top1.shipment_shipped_date
    FROM with_windows s
    LEFT JOIN LATERAL (
            SELECT c.carrier, c.shipped_date AS shipment_shipped_date
            FROM shipments c
            WHERE c.order_id = s.id
            ORDER BY c.shipped_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY shipment_shipped_date DESC NULLS FIRST, id) AS rank_in_group
        FROM with_latest
    ) x
    WHERE rank_in_group = 1
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY amount_rank, id) AS dedup_rank
    FROM latest
    QUALIFY dedup_rank = 1
)
SELECT
    id,
    resolved_channel,
    shipping_fee_avg_3,
    carrier,
    rank_in_group,
    CASE WHEN carrier RLIKE '^(DHL|FedEx)$' THEN 'EXTENDED' WHEN carrier RLIKE '^(Kerry|Flash)$' THEN 'CORE' ELSE 'GROUP_B' END AS carrier_zone
FROM deduped
ORDER BY rank_in_group NULLS FIRST;
