SELECT
    p.id,
    p.launch_date,
    s.supplier_name,
    s.city,
    s.country,
    CONCAT_WS('-', s.supplier_name, s.country) AS supplier_name_country_key
FROM products p
INNER JOIN suppliers s ON s.id = p.supplier_id
WHERE p.brand IS NOT NULL AND p.launch_date BETWEEN '2023-06-01' AND '2025-08-01'
