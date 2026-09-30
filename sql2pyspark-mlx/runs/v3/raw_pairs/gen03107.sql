SELECT
    status,
    MONTH(order_date) AS order_month,
    channel,
    SUM(CASE WHEN status = 'PENDING' THEN amount ELSE 0 END) AS pending_amount
FROM orders
GROUP BY status, order_month, channel;
