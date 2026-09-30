SELECT carrier, delivered_month, cost_count
FROM (
    SELECT carrier, DATE_TRUNC('MONTH', delivered_date) delivered_month, COUNT(cost) cost_count
    FROM shipments
    GROUP BY carrier, DATE_TRUNC('MONTH', delivered_date)
) t
WHERE cost_count > 4
ORDER BY cost_count DESC, delivered_month;
