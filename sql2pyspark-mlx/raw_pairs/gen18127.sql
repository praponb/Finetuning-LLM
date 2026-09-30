SELECT id, signup_date
FROM customers
WHERE segment IN ('SMB', 'Corporate', 'Consumer') AND city IN ('New York', 'Berlin');
