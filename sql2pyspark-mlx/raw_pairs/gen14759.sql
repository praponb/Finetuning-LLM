SELECT launch_date, category, cost, ARRAY_JOIN(tags, ', ') AS tags_text FROM products ORDER BY launch_date DESC, category DESC;
