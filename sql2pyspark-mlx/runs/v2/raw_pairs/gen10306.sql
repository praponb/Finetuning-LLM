SELECT
    o.id,
    o.store_id,
    o.discount,
    c.credit_limit,
    c.country,
    s.region
FROM orders o
FULL OUTER JOIN customers c ON o.customer_id = c.id
INNER JOIN stores s ON s.id = o.store_id
ORDER BY c.credit_limit DESC, id DESC;
