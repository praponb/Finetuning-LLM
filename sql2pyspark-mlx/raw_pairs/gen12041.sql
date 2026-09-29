WITH enriched AS (
    SELECT
        id,
        customer_id,
        store_id,
        status,
        channel,
        amount,
        shipping_fee,
        FIRST_VALUE(discount) IGNORE NULLS OVER (PARTITION BY customer_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_discount,
        AVG(shipping_fee) OVER (PARTITION BY channel ORDER BY shipping_fee, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS shipping_fee_avg_6
    FROM orders
),
metrics AS (
    SELECT
        id,
        customer_id,
        status,
        channel,
        shipping_fee,
        first_known_discount,
        CUME_DIST() OVER (PARTITION BY status ORDER BY first_known_discount NULLS LAST) AS first_known_discount_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY shipping_fee_avg_6, id) AS event_seq,
        LAG(shipping_fee_avg_6, 2) OVER (ORDER BY shipping_fee, id) AS prev_shipping_fee_avg_6
    FROM enriched
    WHERE shipping_fee_avg_6 <= 13
),
with_latest AS (
    SELECT
        s.id,
        s.status,
        s.shipping_fee,
        s.first_known_discount_cume_dist,
        s.event_seq,
        top1.shipment_shipped_date,
        top1.shipment_status
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.shipped_date AS shipment_shipped_date, c.status AS shipment_status
            FROM shipments c
            WHERE c.order_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
latest_match AS (
    SELECT s.*, latest.shipment_status_2
    FROM with_latest s
    LEFT JOIN LATERAL (
            SELECT c.status AS shipment_status_2
            FROM shipments c
            WHERE c.order_id = s.id
            ORDER BY c.delivered_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
eligible AS (
    SELECT s.*
    FROM latest_match s
    WHERE EXISTS (
            SELECT 1
            FROM shipments ch
            WHERE ch.order_id = s.id
              AND ch.delivered_date >= s.shipment_shipped_date
        )
),
kept_keys AS (
    SELECT id, status
    FROM eligible
    INTERSECT ALL
    SELECT id, status
    FROM orders
    WHERE order_date BETWEEN '2024-10-01' AND '2025-10-24'
),
verified AS (
    SELECT s.*
    FROM eligible s
    INNER JOIN kept_keys k ON s.id = k.id AND s.status = k.status
),
eligible_orders AS (
    SELECT s.*
    FROM verified s
    WHERE EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.order_id = s.id
              AND c.unit_price < 390
        )
)
SELECT *
FROM (
    SELECT
        shipment_status_2,
        shipping_fee,
        first_known_discount_cume_dist
    FROM eligible_orders
) src
UNPIVOT (
    value FOR kpi IN (shipping_fee, first_known_discount_cume_dist)
)
