SELECT
    CASE WHEN YEAR(signup_date) < 2024 THEN 'before_2024' WHEN YEAR(signup_date) = 2024 THEN '2024' ELSE 'after_2024' END AS signup_cohort,
    SUM(age) AS total_age,
    ROUND(AVG(credit_limit), 2) AS avg_credit_limit,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(segment))) AS segment_values
FROM customers
WHERE age >= 33 AND signup_date <= '2023-11-01' AND credit_limit > 13700
GROUP BY CASE WHEN YEAR(signup_date) < 2024 THEN 'before_2024' WHEN YEAR(signup_date) = 2024 THEN '2024' ELSE 'after_2024' END
HAVING SUM(age) < 350;
