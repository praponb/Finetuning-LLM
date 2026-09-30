SELECT
    id,
    carrier,
    order_id,
    shipped_date,
    COUNT(*) OVER (PARTITION BY carrier) carrier_count,
    ROW_NUMBER() OVER (PARTITION BY order_id, carrier ORDER BY shipped_date, id) row_num
FROM shipments;
