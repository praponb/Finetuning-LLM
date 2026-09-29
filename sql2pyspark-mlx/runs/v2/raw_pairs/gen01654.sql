SELECT
    id,
    carrier,
    cost,
    order_id,
    DATEDIFF(shipped_date, delivered_date) AS days_delivered_date_to_shipped_date,
    ROUND(cost, 1) AS cost_rounded
FROM shipments
WHERE order_id IS NULL OR cost < 79;
