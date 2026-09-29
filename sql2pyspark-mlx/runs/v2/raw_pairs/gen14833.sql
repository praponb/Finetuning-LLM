SELECT
    p.id,
    p.paid_at,
    o.discount,
    o.customer_id,
    c.is_active,
    INITCAP(TRIM(c.city)) AS city_title
FROM payments p
INNER JOIN orders o ON p.order_id = o.id
FULL OUTER JOIN customers c ON o.customer_id = c.id
WHERE UPPER(c.email) LIKE '%IAM%'
ORDER BY customer_id DESC;
