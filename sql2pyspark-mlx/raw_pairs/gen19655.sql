SELECT
    status,
    store_id,
    channel,
    DATEDIFF(CURRENT_DATE(), order_date) / 365 AS years_since_order_date
FROM orders
WHERE status = 'CANCELLED' AND MONTH(order_date) IN (6, 9, 10)
ORDER BY years_since_order_date DESC;
