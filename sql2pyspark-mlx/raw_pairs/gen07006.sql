SELECT supplier_id FROM products EXCEPT SELECT supplier_id FROM products WHERE LENGTH(product_name) > 6;
