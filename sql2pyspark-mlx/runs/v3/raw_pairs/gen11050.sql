SELECT monthly_fee, 'subscription' AS source
FROM subscriptions
WHERE status IN ('EXPIRED', 'PAUSED', 'ACTIVE')
UNION
SELECT unit_price AS monthly_fee, 'order_item' AS source
FROM order_items
ORDER BY source
