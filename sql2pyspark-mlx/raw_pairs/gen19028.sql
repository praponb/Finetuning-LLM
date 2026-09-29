WITH with_latest AS (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.carrier
    FROM orders s
    JOIN LATERAL (
            SELECT c.carrier
            FROM shipments c
            WHERE c.order_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('YEAR', order_date) AS year_start,
    MAX(shipping_fee) AS max_shipping_fee,
    SUM(amount) FILTER (WHERE channel = 'web') AS web_total_amount,
    SIZE(COLLECT_SET(status)) AS num_status,
    COUNT_IF(channel = 'mobile') AS mobile_events,
    MAX(discount) FILTER (WHERE status = 'RETURNED') AS returned_max_discount
FROM with_latest
GROUP BY DATE_TRUNC('YEAR', order_date);
