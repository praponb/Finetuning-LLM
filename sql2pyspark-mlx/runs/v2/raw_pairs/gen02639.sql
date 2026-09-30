SELECT id, country, credit_limit FROM customers WHERE signup_date <= '2024-06-01' AND age <= 58 AND segment IN ('Corporate', 'SMB', 'Consumer')
