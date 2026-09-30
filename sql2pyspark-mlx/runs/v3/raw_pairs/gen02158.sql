SELECT
    shipments.id,
    shipments.shipped_date,
    shipments.delivered_date,
    orders.store_id,
    CASE WHEN shipments.cost >= 74 THEN 'High' WHEN shipments.cost >= 40 THEN 'Medium' ELSE 'Low' END AS cost_band
FROM shipments
INNER JOIN orders ON shipments.order_id = orders.id
