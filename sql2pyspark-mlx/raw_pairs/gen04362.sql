SELECT *, ROUND(DATEDIFF(delivered_date, shipped_date) / 30, 1) AS months_shipped_date_to_delivered_date, DATEDIFF(shipped_date, delivered_date) AS days_delivered_date_to_shipped_date
FROM shipments
ORDER BY order_id, id;
