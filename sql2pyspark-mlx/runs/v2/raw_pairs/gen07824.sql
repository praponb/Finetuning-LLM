SELECT
    s.id,
    s.store_name,
    s.region,
    s.sqft
FROM stores s
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.store_id = s.id AND o.shipping_fee BETWEEN 30 AND 36)
ORDER BY store_name DESC, region
