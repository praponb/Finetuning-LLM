SELECT *
FROM (
    SELECT DATE_FORMAT(signup_date, 'yyyy-MM') signup_month, MAX(age) max_age
    FROM customers
    GROUP BY DATE_FORMAT(signup_date, 'yyyy-MM')
) agg
WHERE max_age > 38;
