SELECT
    id,
    discount,
    channel,
    DATE_TRUNC('MONTH', order_date) order_date_period
FROM orders
WHERE amount <= 630 AND (channel IS NULL OR order_date >= '2025-05-24')
ORDER BY id DESC, order_date_period
