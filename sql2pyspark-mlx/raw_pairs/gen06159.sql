SELECT CASE WHEN YEAR(c.signup_date) < 2024 THEN 'before_2024' WHEN YEAR(c.signup_date) = 2024 THEN '2024' ELSE 'after_2024' END AS signup_cohort, DATE_TRUNC('MONTH', c.signup_date) AS signup_month, SORT_ARRAY(COLLECT_SET(a.account_type)) AS account_type_list
FROM accounts a
INNER JOIN customers c ON a.customer_id = c.id
WHERE a.customer_id IN (9, 20)
GROUP BY CASE WHEN YEAR(c.signup_date) < 2024 THEN 'before_2024' WHEN YEAR(c.signup_date) = 2024 THEN '2024' ELSE 'after_2024' END, DATE_TRUNC('MONTH', c.signup_date);
