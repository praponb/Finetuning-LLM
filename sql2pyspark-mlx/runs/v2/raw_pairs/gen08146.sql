SELECT opened_date FROM accounts WHERE opened_date >= '2023-09-30' UNION SELECT opened_date FROM accounts WHERE account_type <> 'SAVINGS' ORDER BY opened_date;
