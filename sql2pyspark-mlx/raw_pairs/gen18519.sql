SELECT
    YEAR(signup_date) AS signup_year,
    COUNT(age) AS age_count,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(segment))) AS segment_values,
    MIN(age) AS min_age
FROM customers
GROUP BY YEAR(signup_date)
ORDER BY segment_values ASC
