SELECT signup_date, age FROM customers WHERE MONTH(signup_date) IN (1, 3, 12) EXCEPT SELECT signup_date, age FROM customers WHERE segment NOT IN ('SMB', 'Consumer') ORDER BY signup_date;
