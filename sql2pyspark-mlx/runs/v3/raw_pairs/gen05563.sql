SELECT
    id,
    shipped_date,
    CONCAT_WS('/', carrier, status) carrier_status_key,
    CASE WHEN carrier IN ('UPS', 'Flash') THEN 'Priority' ELSE 'Other' END carrier_group
FROM shipments
WHERE status IN ('DELIVERED', 'LOST') AND order_id IN (18, 21, 24, 30);
