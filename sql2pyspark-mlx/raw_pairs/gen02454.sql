SELECT email
FROM customers
WHERE email IS NOT NULL
EXCEPT
SELECT browser
FROM sessions
WHERE browser IS NOT NULL
ORDER BY email;
