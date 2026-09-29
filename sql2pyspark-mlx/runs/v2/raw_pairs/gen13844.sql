SELECT
    s.id,
    s.rating,
    s.city,
    s.supplier_name
FROM suppliers s
WHERE s.country NOT IN ('UK', 'DE') AND NOT EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id AND p.category NOT IN ('Fashion', 'Books', 'Grocery'))
