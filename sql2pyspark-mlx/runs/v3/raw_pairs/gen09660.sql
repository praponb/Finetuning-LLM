SELECT status, 'shipment' source FROM shipments WHERE order_id IN (2, 27, 30, 37) UNION SELECT status, 'subscription' source FROM subscriptions WHERE monthly_fee BETWEEN 200 AND 210 ORDER BY status;
