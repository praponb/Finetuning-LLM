SELECT
    id,
    cost,
    NTILE(10) OVER (PARTITION BY MONTH(delivered_date) ORDER BY cost DESC, id) AS cost_bucket,
    FIRST_VALUE(cost, TRUE) OVER (ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cost
FROM shipments;
