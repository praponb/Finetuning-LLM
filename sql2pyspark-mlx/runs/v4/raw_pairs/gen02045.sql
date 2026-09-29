SELECT *, INITCAP(TRIM(category)) AS category_title
FROM products
WHERE ARRAY_CONTAINS(tags, 'clearance') OR category = 'Home'
