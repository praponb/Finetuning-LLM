WITH base AS (
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
        COALESCE(amount, 0) amount_filled,
        COALESCE(discount, 0) discount_filled
    FROM orders
    WHERE YEAR(order_date) = 2025
      AND order_date >= DATE '2023-10-09'
),
with_latest AS (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.amount,
        s.discount,
        s.shipping_fee,
        x.shipment_delivered_date
    FROM base s
    JOIN LATERAL (
            SELECT c.delivered_date AS shipment_delivered_date
            FROM shipments c
            WHERE c.order_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.order_id = s.id
              AND ch.unit_price <= 310
        )
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM payments c
            WHERE c.order_id = s.id
              AND c.paid_at <= s.shipment_delivered_date
        )
)
SELECT
    id,
    amount,
    shipping_fee,
    shipment_delivered_date
FROM not_flagged
ORDER BY id DESC NULLS LAST, amount
