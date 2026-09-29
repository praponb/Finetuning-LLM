SELECT
    id,
    delivered_date,
    shipped_date,
    SUBSTRING(status, 1, 4) status_prefix
FROM shipments
WHERE order_id IS NOT NULL AND order_id IN (11, 13);
