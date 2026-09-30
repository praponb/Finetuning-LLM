SELECT
    id,
    order_id,
    carrier,
    shipped_date,
    delivered_date,
    LAG(cost) OVER w_grp AS prev_cost,
    LEAD(cost) OVER w_grp AS next_cost,
    ROW_NUMBER() OVER w_grp AS event_seq
FROM shipments
WHERE order_id IS NULL
WINDOW w_grp AS (PARTITION BY status, carrier ORDER BY delivered_date, id)
