SELECT
    id,
    carrier,
    shipped_date,
    DATEDIFF(CURRENT_DATE(), delivered_date) AS days_since_delivered_date,
    COALESCE(cost, 0) AS cost_filled
FROM shipments
WHERE status = 'DELIVERED'
ORDER BY cost_filled DESC;
