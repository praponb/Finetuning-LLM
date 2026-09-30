SELECT channel, SUM(shipping_fee) total_shipping_fee, RANK() OVER (ORDER BY SUM(shipping_fee) DESC) AS rnk
FROM orders
WHERE order_date <= '2025-10-28'
GROUP BY channel
ORDER BY 2 DESC, channel
LIMIT 5;
