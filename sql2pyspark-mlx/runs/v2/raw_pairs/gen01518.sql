SELECT id, price, COALESCE(product_name, 'unassigned') AS product_name_filled
FROM products
