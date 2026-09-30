SELECT id, category, EXPLODE(tags) AS label FROM products WHERE YEAR(launch_date) >= 2024;
