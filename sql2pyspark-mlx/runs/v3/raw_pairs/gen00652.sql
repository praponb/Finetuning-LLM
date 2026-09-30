SELECT id, credit_limit FROM customers WHERE segment IN ('Corporate', 'SMB', 'Consumer') AND country = 'DE';
