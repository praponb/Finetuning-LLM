SELECT * FROM (SELECT carrier, order_id, status, cost FROM shipments) src PIVOT (MAX(cost) FOR status IN ('LOST', 'RETURNED'));
