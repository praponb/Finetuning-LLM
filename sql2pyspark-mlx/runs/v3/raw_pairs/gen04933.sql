SELECT
    p.id,
    p.status,
    p.order_id,
    o.shipping_fee,
    o.channel,
    s.store_name,
    s.sqft,
    INITCAP(TRIM(s.region)) AS region_title
FROM payments p
INNER JOIN orders o ON p.order_id = o.id
INNER JOIN stores s ON o.store_id = s.id
WHERE s.opened_date BETWEEN '2023-03-31' AND '2025-03-01' OR s.sqft < 6724;
