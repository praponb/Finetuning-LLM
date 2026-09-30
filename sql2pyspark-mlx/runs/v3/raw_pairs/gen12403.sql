SELECT DISTINCT category, brand
FROM products
WHERE supplier_id IS NOT NULL;
