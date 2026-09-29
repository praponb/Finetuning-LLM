SELECT
    MONTH(order_date) AS order_month,
    channel,
    COUNT(*) AS cnt,
    COUNT(CASE WHEN channel = 'partner' THEN 1 END) AS partner_count
FROM orders
GROUP BY order_month, channel
HAVING COUNT(CASE WHEN channel = 'partner' THEN 1 END) >= 3
ORDER BY partner_count ASC, cnt DESC, order_month ASC, channel ASC
LIMIT 1
