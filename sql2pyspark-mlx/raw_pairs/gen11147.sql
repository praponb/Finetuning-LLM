SELECT channel, amount_count, max_shipping_fee FROM (SELECT channel, COUNT(amount) AS amount_count, MAX(shipping_fee) AS max_shipping_fee FROM orders GROUP BY channel) AS sub WHERE amount_count >= 4
