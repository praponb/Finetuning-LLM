SELECT
    customer_id,
    order_date,
    store_id,
    LOWER(channel) AS channel_lower,
    DATEDIFF(CURRENT_DATE(), order_date) AS days_since_order_date
FROM orders
WHERE discount IS NOT NULL AND channel = 'web' AND channel IN ('web', 'partner', 'mobile')
