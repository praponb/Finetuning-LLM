SELECT id, supplier_id, price, ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY price, id) AS price_row FROM products WHERE LENGTH(product_name) >= 7;
