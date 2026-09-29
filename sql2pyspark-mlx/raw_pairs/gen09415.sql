SELECT
    c.id,
    c.city,
    COUNT(o.id) AS order_count,
    ROUND(STDDEV(o.shipping_fee), 2) AS stddev_shipping_fee,
    MAX(o.discount) AS max_discount
FROM customers c
INNER JOIN orders o ON c.id = o.customer_id
WHERE o.discount BETWEEN 0.31 AND 0.35
GROUP BY c.id, c.city
HAVING MAX(o.discount) > 0.25
ORDER BY order_count ASC;
