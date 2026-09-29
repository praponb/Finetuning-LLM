SELECT status FROM accounts WHERE account_type NOT IN ('CHECKING', 'CREDIT', 'SAVINGS') UNION ALL SELECT status FROM payments WHERE status = 'REFUNDED';
