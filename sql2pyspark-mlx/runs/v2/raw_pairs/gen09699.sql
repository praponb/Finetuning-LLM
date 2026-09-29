SELECT
    order_id,
    status,
    CONCAT(carrier, ' ', status) carrier_status,
    SUBSTRING(carrier, 1, 3) carrier_prefix
FROM shipments
WHERE carrier IS NULL
ORDER BY order_id DESC;
