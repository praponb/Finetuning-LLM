SELECT id, category, EXPLODE(tags) AS label FROM products WHERE brand IN ('Initech', 'Globex');
