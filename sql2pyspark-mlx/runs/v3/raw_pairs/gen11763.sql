SELECT
    CASE WHEN country IN ('TH', 'US') THEN 'core' ELSE 'other' END AS country_group,
    SUM(credit_limit) AS total_credit_limit,
    COUNT(*) AS cnt,
    ROUND(SUM(credit_limit) * 100.0 / SUM(SUM(credit_limit)) OVER (), 2) AS pct_of_total
FROM customers
WHERE is_active = FALSE
GROUP BY CASE WHEN country IN ('TH', 'US') THEN 'core' ELSE 'other' END;
