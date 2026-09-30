SELECT status FROM accounts WHERE account_type IN ('CHECKING', 'LOAN') EXCEPT SELECT status FROM payments WHERE amount IS NOT NULL ORDER BY status;
