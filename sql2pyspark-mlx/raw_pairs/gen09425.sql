SELECT id FROM accounts EXCEPT SELECT id FROM accounts WHERE account_type IN ('CHECKING', 'SAVINGS');
