SELECT *, REGEXP_REPLACE(supplier_name, ' ', '_') AS supplier_name_slug, ROUND(rating, 2) AS rating_rounded
FROM suppliers;
