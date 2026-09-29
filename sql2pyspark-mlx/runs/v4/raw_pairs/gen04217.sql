WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount DESC NULLS FIRST, id) AS row_num
    FROM payments
    QUALIFY row_num = 1
)
SELECT
    s.id,
    s.order_id,
    s.paid_at,
    s.status,
    s.row_num,
    CASE WHEN status RLIKE '^(SUCCESS|FAILED)$' THEN 'GROUP_A' WHEN status RLIKE '^(REFUNDED)$' THEN 'CORE' ELSE 'GROUP_B' END AS status_zone,
    (SELECT AVG(x.cost) FROM shipments x WHERE x.order_id = s.order_id) AS avg_shipment_cost
FROM deduped s
ORDER BY status;
