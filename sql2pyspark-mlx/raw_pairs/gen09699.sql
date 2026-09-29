SELECT
    delivered_date,
    order_id,
    carrier,
    TRIM(status) AS clean_status
FROM shipments
