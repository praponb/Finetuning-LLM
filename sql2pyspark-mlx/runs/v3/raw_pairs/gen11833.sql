SELECT
    s.opened_date,
    s.city,
    s.sqft,
    o.customer_id,
    o.discount,
    o.store_id
FROM stores s
LEFT JOIN orders o ON s.id = o.store_id
WHERE s.sqft IS NOT NULL;
