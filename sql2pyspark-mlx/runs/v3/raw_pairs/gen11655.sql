SELECT
    o.id,
    o.channel,
    c.segment,
    s.opened_date,
    s.city
FROM orders o
JOIN customers c ON o.customer_id = c.id
JOIN stores s ON o.store_id = s.id
WHERE c.age <= 49
