SELECT id, category, product_name, SUBSTRING(brand, 1, 3) AS brand_prefix, SORT_ARRAY(tags) AS sorted_tags FROM products;
