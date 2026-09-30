SELECT
    s.country,
    s.supplier_name,
    p.brand,
    p.stock_qty,
    LENGTH(s.country) AS country_length
FROM suppliers s
LEFT OUTER JOIN products p ON p.supplier_id = s.id
ORDER BY country_length DESC, 2 DESC
