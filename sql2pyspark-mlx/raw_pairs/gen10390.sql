SELECT
    CASE WHEN segment IN ('Consumer', 'SMB') THEN 'group_a' ELSE 'other' END AS segment_group,
    MONTH(signup_date) AS signup_month,
    AVG(credit_limit) AS avg_credit_limit,
    COUNT(*) AS total_count
FROM customers
WHERE age BETWEEN 47 AND 61
GROUP BY 1, 2
ORDER BY 1, 3 DESC
