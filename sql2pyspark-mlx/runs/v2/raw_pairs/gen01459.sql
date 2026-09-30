SELECT s.status, o.status AS order_status FROM shipments s JOIN orders o ON o.id = s.order_id WHERE s.delivered_date > '2023-04-21'
