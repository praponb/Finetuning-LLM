SELECT category, product_name, LOWER(brand) brand_lower
FROM products
WHERE category NOT IN ('Grocery', 'Books')
