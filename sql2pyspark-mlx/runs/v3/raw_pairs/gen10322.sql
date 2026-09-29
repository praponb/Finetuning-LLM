SELECT
    country,
    ROUND(STDDEV(age), 2) stddev_age,
    SUM(CASE WHEN country = 'UK' THEN credit_limit ELSE 0 END) uk_credit_limit,
    ROUND(AVG(age), 2) avg_age
FROM customers
WHERE segment IN ('Corporate', 'SMB') AND credit_limit BETWEEN 10500 AND 12400 AND country <> 'SG'
GROUP BY country
