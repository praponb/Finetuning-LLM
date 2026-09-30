WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM payments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY method, order_id ORDER BY amount DESC, id ASC) = 1
)
SELECT
    s.id,
    s.order_id,
    s.method,
    s.status,
    (SELECT SUM(x.cost) FROM shipments x WHERE x.order_id = s.order_id) AS sum_shipment_cost
FROM deduped s
ORDER BY order_id DESC
