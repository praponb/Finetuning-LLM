WITH with_last AS (
    SELECT
        s.*,
        latest.shipment_shipped_date,
        latest.shipment_status
    FROM orders s
    LEFT JOIN LATERAL (
            SELECT c.shipped_date AS shipment_shipped_date, c.status AS shipment_status
            FROM shipments c
            WHERE c.order_id = s.id
            ORDER BY c.delivered_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    store_id,
    MAX(amount) FILTER (WHERE status = 'CANCELLED') AS cancelled_max_amount,
    SUM(CASE WHEN status = 'PENDING' THEN shipping_fee ELSE 0 END) AS pending_shipping_fee,
    SUM(CASE WHEN channel = 'web' THEN shipping_fee ELSE 0 END) AS web_shipping_fee
FROM with_last
GROUP BY store_id;
