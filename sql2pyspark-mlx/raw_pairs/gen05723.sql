SELECT e.id, e.order_id, e.shipped_date, e.delivered_date, e.status, d.order_date, d.status AS order_status FROM shipments e INNER JOIN orders d ON e.order_id = d.id
