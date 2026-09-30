SELECT MONTH(delivered_date) AS delivered_month, COUNT(*) AS shipment_count, ROUND(STDDEV(cost), 2) AS stddev_cost
FROM shipments
WHERE order_id IS NULL AND cost <= 89
GROUP BY delivered_month
HAVING shipment_count < 4
