SELECT location, total_count FROM (SELECT location, COUNT(*) total_count FROM departments GROUP BY location) sub WHERE total_count > 6;
