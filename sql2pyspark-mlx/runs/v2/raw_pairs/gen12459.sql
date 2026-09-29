SELECT is_active, MAX(credit_limit) AS max_credit_limit FROM customers WHERE segment <> 'SMB' AND city = 'Chiang Mai' GROUP BY is_active ORDER BY max_credit_limit;
