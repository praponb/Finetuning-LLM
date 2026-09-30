SELECT account_type, opened_date
FROM accounts
WHERE account_type = 'LOAN'
EXCEPT
SELECT account_type, opened_date
FROM accounts
WHERE balance > 77000;
