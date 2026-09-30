SELECT balance, customer_id, status FROM accounts WHERE account_type IN ('CHECKING', 'SAVINGS', 'LOAN') ORDER BY status;
