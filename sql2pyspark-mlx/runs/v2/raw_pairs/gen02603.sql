SELECT
    id,
    carrier,
    shipped_date,
    ROUND(cost * 1.07, 2) AS cost_scaled
FROM shipments
WHERE order_id IS NULL AND MONTH(delivered_date) IN (2, 7, 12)
ORDER BY cost_scaled;
