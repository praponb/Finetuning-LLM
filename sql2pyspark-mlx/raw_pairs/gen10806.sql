SELECT
    o.id,
    o.shipping_fee,
    o.discount,
    o.order_date,
    s.opened_date,
    s.region,
    s.sqft
FROM orders o
INNER JOIN stores s ON o.store_id = s.id
WHERE s.opened_date > '2023-09-01' OR LOWER(TRIM(s.store_name)) = 'downtown express'
