SELECT
    s.region,
    s.opened_date,
    o.shipping_fee,
    o.store_id,
    DATE_TRUNC('year', o.order_date) AS order_date_period
FROM stores s
LEFT JOIN orders o ON o.store_id = s.id
WHERE s.sqft < 12175
ORDER BY o.store_id DESC
