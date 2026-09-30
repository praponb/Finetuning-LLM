SELECT status, shipping_fee, discount, LENGTH(channel) AS channel_length FROM orders WHERE channel = 'store' OR amount IS NULL;
