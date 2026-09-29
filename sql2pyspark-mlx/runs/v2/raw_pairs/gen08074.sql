SELECT
    DATE_FORMAT(signup_date, 'yyyy-MM') AS signup_month,
    segment,
    city,
    SORT_ARRAY(COLLECT_SET(segment)) AS segment_list,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list
FROM customers
WHERE credit_limit BETWEEN 5000 AND 13400 AND MONTH(signup_date) IN (2, 3, 6) AND signup_date >= '2025-01-17'
GROUP BY 1, 2, 3
ORDER BY city DESC
