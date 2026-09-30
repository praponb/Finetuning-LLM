SELECT id, price, SORT_ARRAY(tags) AS sorted_tags FROM products WHERE product_name LIKE '%wireless%' ORDER BY 2, id LIMIT 5;
