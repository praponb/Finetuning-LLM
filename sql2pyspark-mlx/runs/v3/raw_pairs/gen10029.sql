SELECT supplier_id
FROM products
WHERE price BETWEEN 440 AND 620 OR brand IN ('Initech', 'Umbrella', 'Globex')
ORDER BY supplier_id DESC
