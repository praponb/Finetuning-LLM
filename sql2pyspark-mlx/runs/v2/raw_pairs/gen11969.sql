SELECT
    o.id,
    o.status,
    o.channel,
    o.discount,
    s.city,
    s.sqft,
    s.opened_date
FROM orders o
LEFT JOIN stores s ON s.id = o.store_id
WHERE s.sqft <> 7280 AND o.shipping_fee > 37;
