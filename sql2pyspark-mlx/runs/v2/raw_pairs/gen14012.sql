SELECT
    p.id,
    p.amount,
    o.order_date,
    s.opened_date,
    s.city,
    UPPER(TRIM(s.store_name)) AS store_name_normalized
FROM payments p
LEFT JOIN orders o ON p.order_id = o.id
LEFT JOIN stores s ON o.store_id = s.id
