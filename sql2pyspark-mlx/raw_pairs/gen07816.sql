SELECT launch_date, tags FROM products WHERE category = 'Sports' AND SIZE(tags) > 1 ORDER BY launch_date DESC;
