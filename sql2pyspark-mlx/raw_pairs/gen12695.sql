WITH kept_keys AS (
    SELECT id, status
    FROM shipments
    EXCEPT ALL
    SELECT id, status
    FROM shipments
    WHERE order_id IN (10, 22, 26)
),
reconciled AS (
    SELECT s.*
    FROM shipments s
    JOIN kept_keys k ON s.id = k.id AND s.status = k.status
)
SELECT
    s.id,
    s.order_id,
    s.carrier,
    s.shipped_date,
    s.delivered_date,
    s.cost,
    s.status,
    CASE WHEN status RLIKE '^(RETURNED|LOST)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS status_zone,
    (SELECT MAX(x.cost) FROM shipments x WHERE x.order_id = s.order_id) AS max_shipment_cost
FROM reconciled s;
