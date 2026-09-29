SELECT QUARTER(delivered_date) AS delivered_quarter, DATE_TRUNC('MONTH', shipped_date) AS shipped_month, ROUND(STDDEV(cost), 2) AS stddev_cost
FROM shipments
GROUP BY delivered_quarter, shipped_month
ORDER BY shipped_month DESC
