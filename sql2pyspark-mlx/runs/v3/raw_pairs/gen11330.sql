SELECT id, shipping_fee, status, ROUND(amount * 1.1, 2) AS amount_scaled FROM orders;
