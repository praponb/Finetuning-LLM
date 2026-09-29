SELECT channel, COUNT(shipping_fee) AS shipping_fee_count
FROM orders
WHERE amount <= 990 AND customer_id IS NULL AND shipping_fee < 38
GROUP BY channel
ORDER BY channel
LIMIT 1;
