SELECT
    CASE WHEN YEAR(opened_date) < 2024 THEN 'before_2024' WHEN YEAR(opened_date) = 2024 THEN '2024' ELSE 'after_2024' END AS opened_cohort,
    SUM(balance) AS total_balance,
    AVG(balance) AS avg_balance,
    ROUND(STDDEV(balance), 2) AS stddev_balance
FROM accounts
GROUP BY CASE WHEN YEAR(opened_date) < 2024 THEN 'before_2024' WHEN YEAR(opened_date) = 2024 THEN '2024' ELSE 'after_2024' END
HAVING SUM(balance) < 373000;
