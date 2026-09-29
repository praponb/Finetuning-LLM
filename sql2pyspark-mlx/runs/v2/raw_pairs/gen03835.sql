SELECT
    segment,
    YEAR(signup_date) AS signup_year,
    COUNT(credit_limit) AS credit_limit_count,
    SUM(CASE WHEN country = 'TH' THEN 1 ELSE 0 END) AS th_count
FROM customers
GROUP BY segment, YEAR(signup_date)
