SELECT id, supplier_id, CONCAT_WS('-', brand, product_name) AS brand_product_name_key FROM products ORDER BY brand_product_name_key DESC;
