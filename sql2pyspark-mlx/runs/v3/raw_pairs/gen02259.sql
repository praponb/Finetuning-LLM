SELECT YEAR(signup_date) AS signup_year, ROUND(AVG(credit_limit), 2) AS avg_credit_limit, ROUND((100 * SUM(CASE WHEN country = 'JP' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_jp
FROM customers
WHERE NOT (city = 'London' OR NOT is_active)
GROUP BY YEAR(signup_date);
