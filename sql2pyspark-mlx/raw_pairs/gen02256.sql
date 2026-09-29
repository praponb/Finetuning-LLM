SELECT
    id,
    city,
    INITCAP(TRIM(supplier_name)) AS supplier_name_title,
    CEIL(rating) AS rating_whole
FROM suppliers
WHERE LENGTH(supplier_name) < 12
ORDER BY id DESC
LIMIT 10;
