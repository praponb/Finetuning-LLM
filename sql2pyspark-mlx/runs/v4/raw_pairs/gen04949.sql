SELECT
    ss.id,
    ss.sale_date,
    ss.store_id,
    p.product_name,
    p.cost,
    s.supplier_name,
    s.city,
    DATE_FORMAT(p.launch_date, 'MMM yyyy') AS launch_date_label
FROM store_sales ss
LEFT JOIN products p ON p.id = ss.product_id
JOIN suppliers s ON s.id = p.supplier_id
WHERE s.rating >= 2.6 OR ss.sale_date > '2023-11-01';
