SELECT category, supplier_id, electronics_count
FROM (
    SELECT category, supplier_id, COUNT(CASE WHEN category = 'Electronics' THEN 1 END) AS electronics_count
    FROM products
    GROUP BY category, supplier_id
) AS sub
WHERE electronics_count < 10
ORDER BY electronics_count DESC, category, supplier_id
LIMIT 10;
