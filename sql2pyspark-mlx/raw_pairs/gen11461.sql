SELECT a.account_type, t.merchant FROM accounts a INNER JOIN transactions t ON t.account_id = a.id;
