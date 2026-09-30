SELECT delivered_month, stddev_cost, dhl_cost
FROM (
    SELECT DATE_TRUNC('MONTH', delivered_date) AS delivered_month, ROUND(STDDEV(cost), 2) AS stddev_cost, SUM(CASE WHEN carrier = 'DHL' THEN cost ELSE 0 END) AS dhl_cost
    FROM shipments
    WHERE YEAR(delivered_date) >= 2025
    GROUP BY DATE_TRUNC('MONTH', delivered_date)
) AS agg
WHERE dhl_cost >= 57
ORDER BY dhl_cost;
