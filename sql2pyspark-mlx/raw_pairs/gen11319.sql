SELECT
    CASE WHEN balance >= 60000 THEN 'LARGE' ELSE 'SMALL' END AS balance_tier,
    customer_id,
    DATE_TRUNC('YEAR', opened_date) AS year_start,
    COUNT(CASE WHEN account_type = 'LOAN' THEN 1 END) AS loan_count,
    SUM(CASE WHEN account_type = 'CREDIT' THEN balance ELSE 0 END) AS credit_balance,
    STDDEV_SAMP(balance) AS balance_stddev
FROM accounts
GROUP BY CASE WHEN balance >= 60000 THEN 'LARGE' ELSE 'SMALL' END, customer_id, DATE_TRUNC('YEAR', opened_date);
