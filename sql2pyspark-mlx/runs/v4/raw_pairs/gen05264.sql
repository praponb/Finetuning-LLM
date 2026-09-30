SELECT
    YEAR(signup_date) AS signup_year,
    is_active,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(country))) AS country_values,
    ROUND(STDDEV(credit_limit), 2) AS stddev_credit_limit
FROM customers
WHERE country = 'TH'
GROUP BY 1, 2;
