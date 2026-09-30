SELECT
    s.rating,
    s.city,
    p.launch_date,
    DATE_FORMAT(p.launch_date, 'MMM yyyy') launch_date_label
FROM suppliers s
LEFT JOIN products p ON p.supplier_id = s.id
WHERE s.rating >= 4.0 AND p.product_name IS NOT NULL
ORDER BY rating DESC
