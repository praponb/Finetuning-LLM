SELECT
    segment,
    COUNT_IF(segment = 'SMB') AS smb_events,
    COUNT(CASE WHEN city = 'Bangkok' THEN 1 END) AS bangkok_count,
    CORR(credit_limit, age) AS corr_credit_limit_age,
    APPROX_PERCENTILE(age, ARRAY(0.25, 0.5, 0.75)) AS age_quartiles
FROM customers
GROUP BY segment;
