SELECT id, shipping_fee, amount, LENGTH(channel) AS channel_length FROM orders WHERE customer_id IS NULL AND discount IS NOT NULL
