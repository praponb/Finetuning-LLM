SELECT
    MONTH(a.opened_date) AS opened_month,
    CASE WHEN YEAR(a.opened_date) < 2024 THEN 'before_2024' WHEN YEAR(a.opened_date) = 2024 THEN '2024' ELSE 'after_2024' END AS opened_cohort,
    COUNT(t.amount) AS amount_count,
    COUNT(*) AS total_count
FROM transactions t
JOIN accounts a ON t.account_id = a.id
WHERE a.account_type IN ('LOAN', 'SAVINGS', 'CHECKING')
GROUP BY MONTH(a.opened_date), CASE WHEN YEAR(a.opened_date) < 2024 THEN 'before_2024' WHEN YEAR(a.opened_date) = 2024 THEN '2024' ELSE 'after_2024' END
ORDER BY amount_count DESC
