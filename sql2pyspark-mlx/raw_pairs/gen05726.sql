SELECT id, shipping_fee, SUBSTRING(status, 1, 4) AS status_prefix FROM orders WHERE (customer_id IN (1, 25) AND status NOT IN ('PENDING', 'CANCELLED')) OR discount <= 0.14
