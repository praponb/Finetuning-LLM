SELECT channel, shipping_fee_range FROM (SELECT channel, MAX(shipping_fee) - MIN(shipping_fee) AS shipping_fee_range FROM orders GROUP BY channel) AS t WHERE shipping_fee_range >= 21
