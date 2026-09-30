WITH latest_match AS (
    SELECT
        s.*,
        x.shipment_shipped_date,
        x.shipment_cost
    FROM orders s
    JOIN LATERAL (
            SELECT c.shipped_date AS shipment_shipped_date, c.cost AS shipment_cost
            FROM shipments c
            WHERE c.order_id = s.id
              AND c.shipped_date <= s.order_date
            ORDER BY c.shipped_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    s.id,
    s.customer_id,
    s.store_id,
    s.order_date,
    s.status,
    s.shipping_fee,
    s.shipment_cost,
    CASE WHEN status RLIKE '^(SHIPPED|RETURNED)$' THEN 'EXTENDED' WHEN status RLIKE '^(COMPLETED|PENDING)$' THEN 'CORE' ELSE 'GROUP_A' END AS status_zone,
    (SELECT SUM(t2.monthly_fee) FROM subscriptions t2 WHERE t2.customer_id = s.customer_id) AS sum_subscription_monthly_fee
FROM latest_match s
