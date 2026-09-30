SELECT signup_date
FROM customers
WHERE segment IN ('Corporate', 'SMB')
EXCEPT
SELECT signup_date
FROM customers
WHERE NOT is_active
ORDER BY signup_date
