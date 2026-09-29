SELECT
    o.id,
    o.order_date,
    o.channel,
    c.city,
    c.signup_date,
    s.store_name,
    SUBSTRING(o.channel, 1, 3) AS channel_prefix
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.id
INNER JOIN stores s ON o.store_id = s.id
WHERE s.opened_date <= '2024-03-01' OR o.channel IN ('partner', 'store', 'web')
