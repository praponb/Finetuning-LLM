SELECT
    s.supplier_name,
    p.cost,
    p.product_name,
    CONCAT(s.country, ' ', s.supplier_name) AS country_supplier_name
FROM suppliers s
LEFT JOIN products p ON s.id = p.supplier_id
WHERE s.rating < 1.8
