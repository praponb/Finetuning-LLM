SELECT CASE WHEN YEAR(a.opened_date) < 2024 THEN 'before_2024' WHEN YEAR(a.opened_date) = 2024 THEN '2024' ELSE 'after_2024' END AS opened_cohort, a.account_type, SUM(t.amount) AS total_amount
FROM transactions t
INNER JOIN accounts a ON a.id = t.account_id
WHERE a.balance >= 32000
GROUP BY CASE WHEN YEAR(a.opened_date) < 2024 THEN 'before_2024' WHEN YEAR(a.opened_date) = 2024 THEN '2024' ELSE 'after_2024' END, a.account_type
