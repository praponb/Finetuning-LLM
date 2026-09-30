SELECT DATE_TRUNC('MONTH', shipped_date) AS shipped_month, SUM(cost) AS total_cost, ROUND(SUM(cost) * 100.0 / SUM(SUM(cost)) OVER (), 2) AS pct_of_total
FROM shipments
GROUP BY DATE_TRUNC('MONTH', shipped_date)
