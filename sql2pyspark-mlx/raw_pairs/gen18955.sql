WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY delivered_date DESC, id) AS seq_no,
        CUME_DIST() OVER (PARTITION BY carrier ORDER BY cost DESC) AS cost_cume_dist
    FROM shipments
    WHERE carrier IN ('UPS', 'FedEx', 'Flash')
)
SELECT
    s.id,
    s.order_id,
    s.carrier,
    s.cost,
    s.seq_no,
    s.cost_cume_dist,
    (SELECT MAX(t2.quantity) FROM order_items t2 WHERE t2.order_id = s.order_id) AS max_order_item_quantity
FROM scored s
ORDER BY max_order_item_quantity DESC NULLS FIRST;
