SELECT SUM(CASE WHEN country = 'US' THEN credit_limit ELSE 0 END) us_credit_limit, MAX(age) - MIN(age) age_range FROM customers
