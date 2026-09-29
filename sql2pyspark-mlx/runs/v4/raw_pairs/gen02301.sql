SELECT *, COALESCE(cost, 0) cost_filled, DATEDIFF(shipped_date, delivered_date) days_delivered_date_to_shipped_date
FROM shipments
WHERE cost BETWEEN 60 AND 72
