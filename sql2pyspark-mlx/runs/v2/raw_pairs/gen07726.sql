SELECT channel, MAX(order_date) AS latest_order_date, COUNT(discount) AS discount_count
FROM orders
WHERE (amount >= 930 AND store_id IS NULL) OR channel != 'partner'
GROUP BY channel
ORDER BY discount_count DESC, 1 ASC
