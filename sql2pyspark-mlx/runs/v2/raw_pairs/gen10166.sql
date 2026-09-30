SELECT signup_date, email FROM customers WHERE segment NOT IN ('Corporate', 'Consumer') INTERSECT SELECT signup_date, email FROM customers WHERE credit_limit >= 13000
