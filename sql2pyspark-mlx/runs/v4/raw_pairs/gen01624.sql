SELECT
    id,
    order_id,
    CASE WHEN carrier = 'Kerry' THEN 1 ELSE 0 END AS is_kerry,
    DATE_FORMAT(shipped_date, 'yyyy-MM-dd') AS shipped_date_label
FROM shipments
WHERE status IN ('RETURNED', 'LOST', 'IN_TRANSIT');
