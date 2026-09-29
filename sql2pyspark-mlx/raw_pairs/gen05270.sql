SELECT
    DATE_FORMAT(signup_date, 'yyyy-MM') AS signup_month,
    CASE WHEN credit_limit >= 15100 THEN 'high' WHEN credit_limit >= 7300 THEN 'mid' ELSE 'low' END AS credit_limit_bucket,
    segment,
    ROUND(AVG(credit_limit), 2) AS avg_credit_limit,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) AS city_values,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(country))) AS country_values
FROM customers
GROUP BY signup_month, credit_limit_bucket, segment
HAVING ROUND(AVG(credit_limit), 2) < 62
ORDER BY segment DESC, avg_credit_limit DESC, signup_month, credit_limit_bucket
LIMIT 1;
