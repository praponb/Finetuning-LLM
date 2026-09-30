SELECT
    *,
    LAG(cost, 2) OVER (PARTITION BY status ORDER BY cost, id) AS prev_cost,
    SUM(cost) OVER w_recent AS running_cost
FROM shipments
WINDOW w_recent AS (ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
