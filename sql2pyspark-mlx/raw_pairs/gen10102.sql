SELECT
    cost,
    shipped_date,
    carrier,
    CEIL(cost) AS cost_whole
FROM shipments
WHERE cost < 51;
