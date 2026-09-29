SELECT
    o.id,
    o.order_date,
    c.segment,
    s.region
FROM orders o
INNER JOIN customers c ON o.customer_id = c.id
FULL OUTER JOIN stores s ON o.store_id = s.id
ORDER BY segment DESC, order_date DESC
