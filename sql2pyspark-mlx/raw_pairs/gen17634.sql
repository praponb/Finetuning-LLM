SELECT
    CASE WHEN YEAR(opened_date) < 2024 THEN 'before_2024' WHEN YEAR(opened_date) = 2024 THEN '2024' ELSE 'after_2024' END AS opened_cohort,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values,
    MAX(balance) - MIN(balance) AS balance_range,
    ROUND(AVG(balance), 2) AS avg_balance
FROM accounts
WHERE opened_date < '2024-08-01' OR status NOT IN ('CLOSED', 'OPEN')
GROUP BY CASE WHEN YEAR(opened_date) < 2024 THEN 'before_2024' WHEN YEAR(opened_date) = 2024 THEN '2024' ELSE 'after_2024' END
ORDER BY opened_cohort ASC
LIMIT 50
