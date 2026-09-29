SELECT CASE WHEN segment IN ('Corporate', 'SMB') THEN 'priority' ELSE 'other' END AS segment_group, SUM(credit_limit) AS total_credit_limit, ROUND(STDDEV(credit_limit), 2) AS stddev_credit_limit
FROM customers
GROUP BY 1
HAVING MAX(age) > 43;
