SELECT *, INITCAP(TRIM(product_name)) AS product_name_title FROM products WHERE category != 'Sports';
