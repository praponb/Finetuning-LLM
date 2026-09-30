SELECT
    suppliers.id,
    suppliers.country,
    suppliers.rating,
    t.min_launch_date,
    t.fashion_count
FROM suppliers
INNER JOIN (SELECT supplier_id, MIN(launch_date) min_launch_date, SUM(CASE WHEN category = 'Fashion' THEN 1 ELSE 0 END) fashion_count
    FROM products
    WHERE stock_qty BETWEEN 124 AND 318
    GROUP BY supplier_id) t ON suppliers.id = t.supplier_id
ORDER BY fashion_count DESC, id
LIMIT 10;
