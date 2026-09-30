SELECT
    status,
    channel,
    MAX(discount) - MIN(discount) AS discount_range,
    COUNT(CASE WHEN status = 'RETURNED' THEN 1 END) AS returned_count
FROM orders
WHERE amount >= 1100 AND (YEAR(order_date) = 2025 OR shipping_fee > 28)
GROUP BY status, channel;
