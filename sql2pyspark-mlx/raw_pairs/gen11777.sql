SELECT
    id,
    shipped_date,
    delivered_date,
    LAG(cost) OVER w_recent AS prev_cost,
    ROW_NUMBER() OVER w_recent AS row_seq
FROM shipments
WINDOW w_recent AS (PARTITION BY status, carrier ORDER BY cost, id)
