SELECT * FROM (SELECT order_id, carrier, cost FROM shipments WHERE order_id IS NOT NULL) PIVOT (AVG(cost) FOR carrier IN ('Flash', 'FedEx')) ORDER BY order_id
