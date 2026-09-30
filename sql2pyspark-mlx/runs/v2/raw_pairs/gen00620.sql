SELECT channel, MIN(shipping_fee) AS min_shipping_fee, MIN(amount) AS min_amount FROM orders GROUP BY 1
