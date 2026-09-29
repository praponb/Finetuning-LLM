SELECT id, price, ARRAY_JOIN(tags, ', ') AS tags_text, SORT_ARRAY(tags) AS sorted_tags FROM products WHERE cost < 720 AND ARRAY_CONTAINS(tags, 'bestseller');
