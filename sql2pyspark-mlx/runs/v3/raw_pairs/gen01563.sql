SELECT
    o.id,
    o.channel,
    s.region,
    c.country,
    c.email
FROM orders o
INNER JOIN stores s ON o.store_id = s.id
INNER JOIN customers c ON c.id = o.customer_id
WHERE s.city IN ('Bangkok', 'Tokyo') AND o.amount > 1300
ORDER BY o.channel DESC, id ASC
LIMIT 20;
