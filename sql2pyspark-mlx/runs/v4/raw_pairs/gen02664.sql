SELECT e.*, a.balance, a.account_type, a.status FROM transactions e LEFT JOIN accounts a ON e.account_id = a.id;
