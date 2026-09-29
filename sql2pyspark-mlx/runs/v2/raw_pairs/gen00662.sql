SELECT
    id,
    status,
    order_id,
    DATE_TRUNC('MONTH', shipped_date) AS shipped_date_period
FROM shipments
WHERE carrier IS NOT NULL OR cost BETWEEN 33 AND 73;
