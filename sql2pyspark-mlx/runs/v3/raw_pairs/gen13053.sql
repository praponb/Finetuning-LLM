SELECT * FROM (SELECT status, COUNT(*) AS total_count FROM accounts GROUP BY status) AS x WHERE total_count < 6 ORDER BY total_count DESC;
