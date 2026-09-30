SELECT status
FROM accounts
WHERE YEAR(opened_date) >= 2023
EXCEPT
SELECT status
FROM payments
ORDER BY status DESC;
