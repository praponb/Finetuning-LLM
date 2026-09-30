SELECT id, category, SORT_ARRAY(tags) AS sorted_tags
FROM products
ORDER BY id;
