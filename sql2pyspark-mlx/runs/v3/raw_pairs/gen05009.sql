SELECT
    s.id,
    s.delivered_date,
    o.channel,
    o.customer_id,
    DATEDIFF(CURRENT_DATE(), s.shipped_date) AS days_since_shipped_date
FROM shipments s
JOIN orders o ON s.order_id = o.id
WHERE o.shipping_fee BETWEEN 16 AND 36 AND s.order_id IS NULL
