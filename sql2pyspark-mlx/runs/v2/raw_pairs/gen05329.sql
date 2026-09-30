SELECT
    ss.product_id,
    ss.sale_date,
    p.cost,
    p.category,
    s.rating
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
INNER JOIN suppliers s ON s.id = p.supplier_id
ORDER BY category, ss.sale_date;
