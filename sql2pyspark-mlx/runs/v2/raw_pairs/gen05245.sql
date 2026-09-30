SELECT a.opened_date, a.status, t.merchant, t.is_flagged FROM accounts a LEFT JOIN transactions t ON t.account_id = a.id WHERE a.account_type = 'CREDIT';
