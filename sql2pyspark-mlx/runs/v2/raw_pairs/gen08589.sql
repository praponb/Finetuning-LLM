SELECT
    id,
    order_id,
    carrier,
    DATE_FORMAT(delivered_date, 'yyyy-MM-dd') AS delivered_date_label
FROM shipments;
