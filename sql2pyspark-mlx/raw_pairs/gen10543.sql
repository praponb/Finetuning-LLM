WITH cleaned AS (
    SELECT
        *,
        CASE WHEN discount >= 0.39 THEN 'High' WHEN discount >= 0.07 THEN 'Medium' ELSE 'Low' END AS discount_band,
        DATEDIFF(CURRENT_DATE(), order_date) AS days_since_order_date,
        COALESCE(channel, 'N/A') AS resolved_channel
    FROM orders
    WHERE order_date > DATE '2023-07-28'
      AND channel IS DISTINCT FROM 'partner'
)
SELECT
    s.id,
    s.customer_id,
    s.store_id,
    s.order_date,
    s.status,
    s.amount,
    s.discount,
    s.resolved_channel,
    top1.delivered_date,
    top1.shipment_shipped_date
FROM cleaned s
LEFT JOIN LATERAL (
        SELECT c.delivered_date, c.shipped_date AS shipment_shipped_date
        FROM shipments c
        WHERE c.order_id = s.id
          AND c.shipped_date <= s.order_date
        ORDER BY c.shipped_date DESC, c.id DESC
        LIMIT 1
    ) top1 ON TRUE;
