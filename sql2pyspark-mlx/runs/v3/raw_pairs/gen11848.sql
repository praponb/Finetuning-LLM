SELECT YEAR(end_date) AS end_year, COUNT(CASE WHEN plan = 'basic' THEN 1 END) AS basic_count FROM subscriptions WHERE plan = 'premium' OR end_date BETWEEN '2024-02-01' AND '2025-08-01' GROUP BY 1;
