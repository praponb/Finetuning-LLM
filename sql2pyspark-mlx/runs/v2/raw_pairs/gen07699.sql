SELECT
    id,
    shipped_date,
    ROUND(cost * 100, 2) AS adjusted_cost,
    ROUND(cost * 100, 2) AS adjusted_cost_2
FROM shipments
WHERE cost BETWEEN 76 AND 95 AND status != 'RETURNED'
ORDER BY 2, id
