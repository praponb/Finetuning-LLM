SELECT
    is_active,
    country,
    segment,
    SUM(CASE WHEN city = 'Berlin' THEN age ELSE 0 END) AS berlin_age,
    ROUND(STDDEV(credit_limit), 2) AS stddev_credit_limit
FROM customers
GROUP BY is_active, country, segment
ORDER BY segment DESC, stddev_credit_limit DESC;
