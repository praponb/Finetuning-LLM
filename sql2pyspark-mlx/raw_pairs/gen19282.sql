SELECT status, MAX(delivered_date) AS latest_delivered_date, MAX(cost) - MIN(cost) AS cost_range
FROM shipments
WHERE carrier <> 'Flash' AND order_id IS NULL
GROUP BY status;
