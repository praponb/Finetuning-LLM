SELECT country, segment
FROM customers
EXCEPT DISTINCT
SELECT country, segment
FROM customers
WHERE UPPER(TRIM(email)) = 'ALICE@GMAIL.COM'
