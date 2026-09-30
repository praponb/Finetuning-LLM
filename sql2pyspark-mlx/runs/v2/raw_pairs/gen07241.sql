SELECT *
FROM (
    SELECT is_active, DATE_FORMAT(signup_date, 'yyyy-MM') signup_month, COUNT(age) age_count
    FROM customers
    GROUP BY is_active, DATE_FORMAT(signup_date, 'yyyy-MM')
) agg
WHERE age_count > 8
