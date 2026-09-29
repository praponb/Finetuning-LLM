SELECT signup_month, credit_limit_count
FROM (
    SELECT MONTH(signup_date) AS signup_month, COUNT(credit_limit) AS credit_limit_count
    FROM customers
    WHERE segment = 'SMB'
    GROUP BY MONTH(signup_date)
) AS agg
WHERE credit_limit_count < 5
