SELECT
    QUARTER(signup_date) AS signup_quarter,
    is_active,
    DATE_FORMAT(signup_date, 'yyyy-MM') AS signup_month,
    COUNT(age) AS age_count,
    SUM(CASE WHEN country = 'SG' THEN credit_limit ELSE 0 END) AS sg_credit_limit
FROM customers
GROUP BY QUARTER(signup_date), is_active, DATE_FORMAT(signup_date, 'yyyy-MM');
