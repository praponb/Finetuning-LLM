SELECT SUM(CASE WHEN segment = 'Corporate' THEN credit_limit ELSE 0 END) AS corporate_credit_limit, MAX(age) - MIN(age) AS age_range
FROM customers
ORDER BY corporate_credit_limit DESC
