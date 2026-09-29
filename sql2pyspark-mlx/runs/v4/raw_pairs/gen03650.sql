SELECT
    account_type,
    status,
    CASE WHEN YEAR(opened_date) < 2024 THEN 'before_2024' WHEN YEAR(opened_date) = 2024 THEN '2024' ELSE 'after_2024' END AS opened_cohort,
    ROUND(STDDEV(balance), 2) AS stddev_balance
FROM accounts
WHERE status IN ('CLOSED', 'OPEN', 'FROZEN')
GROUP BY account_type, status, CASE WHEN YEAR(opened_date) < 2024 THEN 'before_2024' WHEN YEAR(opened_date) = 2024 THEN '2024' ELSE 'after_2024' END
