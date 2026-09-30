SELECT id, tags, DATEDIFF(CURRENT_DATE(), launch_date) AS days_since_launch_date, ARRAY_JOIN(tags, '|') AS tags_text FROM products WHERE brand = 'Initech';
