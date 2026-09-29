SELECT
    o.channel,
    DATE_FORMAT(o.order_date, 'yyyy-MM') order_month,
    ROUND((100 * SUM(CASE WHEN s.carrier = 'FedEx' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_fedex,
    COUNT(CASE WHEN s.carrier = 'DHL' THEN 1 END) dhl_count
FROM shipments s
INNER JOIN orders o ON o.id = s.order_id
GROUP BY o.channel, DATE_FORMAT(o.order_date, 'yyyy-MM')
HAVING dhl_count > 2
