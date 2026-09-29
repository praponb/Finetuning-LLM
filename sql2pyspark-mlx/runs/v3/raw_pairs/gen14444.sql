SELECT is_active, SUM(age) AS total_age, COUNT(*) AS cnt, SUM(SUM(age)) OVER (ORDER BY is_active) AS cumulative_age FROM customers GROUP BY is_active
