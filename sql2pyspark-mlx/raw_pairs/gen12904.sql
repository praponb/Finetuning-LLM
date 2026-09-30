WITH not_flagged AS (
    SELECT s.*
    FROM orders s
    WHERE EXISTS (
            SELECT 1
            FROM shipments c
            WHERE c.order_id = s.id
              AND c.cost > 96
        )
),
enriched AS (
    SELECT
        s.*,
        d.store_name,
        d.sqft
    FROM not_flagged s
    INNER JOIN stores d
        ON s.store_id = d.id
    WHERE d.opened_date >= '2024-02-12'
),
with_dims AS (
    SELECT
        a.*,
        d.email,
        d.signup_date
    FROM enriched a
    INNER JOIN customers d
        ON a.customer_id = d.id
),
with_last AS (
    SELECT s.*, x.shipment_cost
    FROM with_dims s
    LEFT JOIN LATERAL (
            SELECT c.cost AS shipment_cost
            FROM shipments c
            WHERE c.order_id = s.id
              AND c.shipped_date <= s.order_date
            ORDER BY c.shipped_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    order_date,
    status,
    amount,
    shipping_fee,
    sqft,
    shipment_cost,
    CASE WHEN status RLIKE '^(PENDING|COMPLETED)$' THEN 'GROUP_B' WHEN status RLIKE '^(CANCELLED|SHIPPED)$' THEN 'GROUP_A' ELSE 'OTHER_ZONE' END AS status_zone
FROM with_last;
