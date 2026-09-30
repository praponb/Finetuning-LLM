SELECT CASE WHEN YEAR(a.opened_date) < 2024 THEN 'before_2024' WHEN YEAR(a.opened_date) = 2024 THEN '2024' ELSE 'after_2024' END AS opened_cohort, MONTH(a.opened_date) AS opened_month, MAX(t.amount) - MIN(t.amount) AS amount_range
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
GROUP BY CASE WHEN YEAR(a.opened_date) < 2024 THEN 'before_2024' WHEN YEAR(a.opened_date) = 2024 THEN '2024' ELSE 'after_2024' END, MONTH(a.opened_date)
