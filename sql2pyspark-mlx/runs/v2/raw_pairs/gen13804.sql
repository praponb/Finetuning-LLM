SELECT ROUND((100 * SUM(CASE WHEN category = 'Home' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_home FROM products WHERE NOT (ARRAY_CONTAINS(tags, 'eco') AND price >= 310)
