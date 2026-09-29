SELECT a.balance, a.account_type, a.opened_date, t.merchant FROM accounts a LEFT JOIN transactions t ON a.id = t.account_id WHERE a.balance <= 49000;
