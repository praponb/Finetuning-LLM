SELECT id, cost, order_id, shipped_date FROM shipments WHERE cost >= (SELECT MAX(cost) FROM shipments WHERE delivered_date > '2025-05-03') ORDER BY id DESC
