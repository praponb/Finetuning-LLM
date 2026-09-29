SELECT o.id, o.shipping_fee, s.sqft
FROM orders o
JOIN stores s ON o.store_id = s.id
WHERE o.amount IS NOT NULL AND YEAR(s.opened_date) = 2025;
