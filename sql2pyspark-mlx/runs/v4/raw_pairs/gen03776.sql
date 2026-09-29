SELECT
    s.opened_date,
    s.city,
    s.region,
    o.shipping_fee,
    o.amount,
    o.channel
FROM stores s
LEFT JOIN orders o ON s.id = o.store_id
WHERE s.region IS NOT NULL
ORDER BY 6;
