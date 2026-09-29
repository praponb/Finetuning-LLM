SELECT *, DATE_SUB(opened_date, 80) AS opened_date_minus_80d, YEAR(opened_date) AS opened_date_year
FROM accounts
WHERE account_type = 'CREDIT'
ORDER BY account_type, balance DESC, id
LIMIT 3;
