SELECT QUARTER(shipped_date) AS shipped_quarter, MAX(cost) - MIN(cost) AS cost_range, SUM(CASE WHEN carrier = 'FedEx' THEN 1 ELSE 0 END) AS fedex_count
FROM shipments
GROUP BY QUARTER(shipped_date)
