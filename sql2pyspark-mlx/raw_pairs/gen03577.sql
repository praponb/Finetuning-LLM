SELECT MONTH(shipped_date) AS shipped_month, MIN(cost) AS min_cost
FROM shipments
GROUP BY MONTH(shipped_date)
ORDER BY shipped_month, min_cost;
