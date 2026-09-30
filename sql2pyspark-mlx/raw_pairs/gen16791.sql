SELECT
    o.id,
    o.amount,
    c.segment,
    COALESCE(c.segment, 'N/A') AS segment_filled
FROM orders o
INNER JOIN customers c ON c.id = o.customer_id
WHERE c.email LIKE '%@gmail.com' OR o.customer_id IN (2, 4, 12)
ORDER BY segment
