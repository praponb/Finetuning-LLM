SELECT *, LENGTH(brand) AS brand_length
FROM products
WHERE LENGTH(product_name) < 12 AND product_name IS NULL
