SELECT customer_id, status FROM accounts WHERE balance BETWEEN 68000 AND 76000 EXCEPT SELECT customer_id, status FROM accounts WHERE account_type IN ('CHECKING', 'CREDIT') ORDER BY status;
