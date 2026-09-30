SELECT
    s.id,
    s.sqft,
    s.opened_date,
    s.store_name
FROM stores s
WHERE s.opened_date >= '2023-10-30' AND NOT EXISTS (SELECT 1 FROM orders o WHERE o.store_id = s.id AND o.shipping_fee > 22)
