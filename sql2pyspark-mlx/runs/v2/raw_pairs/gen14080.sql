SELECT
    o.channel,
    c.segment,
    c.city,
    s.city AS store_city
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.id
FULL OUTER JOIN stores s ON o.store_id = s.id
WHERE c.age <> 62
ORDER BY store_city DESC, city DESC
