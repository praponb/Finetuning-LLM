SELECT *, TRIM(email) AS clean_email
FROM customers
WHERE country = 'UK' AND country != 'JP';
