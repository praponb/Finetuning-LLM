SELECT signup_quarter, min_credit_limit, latest_signup_date
FROM (
    SELECT QUARTER(signup_date) AS signup_quarter, MIN(credit_limit) AS min_credit_limit, MAX(signup_date) AS latest_signup_date
    FROM customers
    GROUP BY QUARTER(signup_date)
) AS x
WHERE min_credit_limit >= 9800
ORDER BY min_credit_limit DESC, signup_quarter
LIMIT 3
