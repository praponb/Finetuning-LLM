SELECT id, tags, UPPER(product_name) product_name_upper FROM products ORDER BY product_name_upper, id DESC;
