SELECT
    o.store_id,
    o.customer_id,
    s.region,
    s.city
FROM orders o
RIGHT JOIN stores s ON o.store_id = s.id
WHERE s.region IN ('South', 'Central') OR o.order_date BETWEEN '2024-07-09' AND '2025-07-01'
