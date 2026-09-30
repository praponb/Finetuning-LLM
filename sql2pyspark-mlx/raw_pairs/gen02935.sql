WITH scored AS (
    SELECT
        *,
        LEAD(cost, 1) OVER w_part AS next_cost,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY shipped_date, id) AS row_seq,
        DENSE_RANK() OVER w_recent AS cost_dense_rank
    FROM shipments
    WINDOW w_part AS (PARTITION BY carrier ORDER BY shipped_date, id), w_recent AS (PARTITION BY carrier, order_id ORDER BY cost DESC NULLS LAST)
)
SELECT
    s.id,
    s.order_id,
    s.shipped_date,
    s.cost,
    s.status,
    s.cost_dense_rank,
    (SELECT MAX(s2.cost) FROM shipments s2 WHERE s2.order_id = s.order_id) AS max_shipment_cost
FROM scored s
ORDER BY cost DESC
