SELECT
    s.id,
    s.supplier_name,
    s.country,
    t.unique_categorys,
    t.stark_price
FROM suppliers s
INNER JOIN (SELECT supplier_id, COUNT(DISTINCT stock_qty) AS unique_categorys, SUM(CASE WHEN brand = 'Stark' THEN price ELSE 0 END) AS stark_price
    FROM products
    GROUP BY supplier_id) t ON s.id = t.supplier_id
WHERE t.unique_categorys >= 5
ORDER BY 1, unique_categorys
LIMIT 3;
