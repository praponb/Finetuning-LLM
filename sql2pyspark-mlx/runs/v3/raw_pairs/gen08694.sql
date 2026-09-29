select id, product_name, trim(product_name) as clean_product_name from products where cost is not null;
