SELECT id, cost, price, category, SIZE(tags) AS tags_count FROM products WHERE category <> 'Grocery';
