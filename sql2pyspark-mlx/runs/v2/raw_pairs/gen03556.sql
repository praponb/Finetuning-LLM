SELECT YEAR(signup_date) AS signup_year, ROUND((100 * SUM(CASE WHEN city = 'Tokyo' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_tokyo, COUNT(*) AS cnt FROM customers GROUP BY signup_year;
