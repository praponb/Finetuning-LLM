SELECT
    p.product_name,
    p.launch_date,
    p.stock_qty,
    s.country,
    TRIM(p.category) AS clean_category
FROM products p
INNER JOIN suppliers s ON s.id = p.supplier_id
WHERE s.rating > 4.2 AND p.price > 1180
ORDER BY 4 ASC;
