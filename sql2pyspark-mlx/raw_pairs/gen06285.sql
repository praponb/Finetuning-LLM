SELECT channel, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(channel))) AS channel_values, ROUND(AVG(shipping_fee), 2) AS avg_shipping_fee
FROM orders
WHERE channel = 'web'
GROUP BY 1
HAVING ROUND(AVG(shipping_fee), 2) < 43
ORDER BY channel
