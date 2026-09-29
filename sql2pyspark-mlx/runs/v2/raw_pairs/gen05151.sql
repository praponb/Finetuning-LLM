SELECT
    id,
    credit_limit,
    CASE WHEN credit_limit >= 14600 THEN 'High' WHEN credit_limit >= 5600 THEN 'Medium' ELSE 'Low' END AS credit_limit_band,
    age - credit_limit AS age_minus_credit_limit
FROM customers
WHERE is_active = TRUE AND credit_limit < 7400 AND country IN ('DE', 'TH')
ORDER BY 2 DESC, 3 DESC;
