SELECT *, SUBSTRING(product_name, 1, 3) AS product_name_prefix FROM products WHERE price >= 1200 AND ARRAY_CONTAINS(tags, 'bestseller')
