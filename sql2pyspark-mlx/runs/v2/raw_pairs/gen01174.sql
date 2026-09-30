SELECT
    id,
    delivered_date,
    carrier,
    status,
    shipped_date,
    cost,
    ROW_NUMBER() OVER (ORDER BY delivered_date, id) AS row_num,
    LAG(cost, 1) OVER (PARTITION BY carrier, status ORDER BY shipped_date, id) AS prev_cost
FROM shipments;
