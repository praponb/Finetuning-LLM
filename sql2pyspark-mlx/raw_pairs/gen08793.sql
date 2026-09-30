SELECT account_type FROM accounts WHERE YEAR(opened_date) = 2023 UNION SELECT account_type FROM accounts WHERE balance >= 31000 ORDER BY account_type;
