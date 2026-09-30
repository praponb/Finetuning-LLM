SELECT
    o.customer_id,
    o.channel,
    c.name,
    s.region,
    s.sqft
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.id
FULL OUTER JOIN stores s ON o.store_id = s.id
WHERE YEAR(s.opened_date) = 2025
ORDER BY customer_id ASC;
