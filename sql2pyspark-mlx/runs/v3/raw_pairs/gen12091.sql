SELECT shipping_fee, channel, ROUND(amount * 0.85, 2) AS amount_scaled FROM orders;
