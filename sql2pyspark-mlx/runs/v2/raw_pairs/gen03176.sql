SELECT
    i.id,
    i.last_restock,
    p.product_name,
    s.supplier_name,
    s.country,
    SUBSTRING(s.supplier_name, 1, 4) AS supplier_name_prefix
FROM inventory i
FULL OUTER JOIN products p ON i.product_id = p.id
LEFT JOIN suppliers s ON p.supplier_id = s.id
ORDER BY 1 DESC;
