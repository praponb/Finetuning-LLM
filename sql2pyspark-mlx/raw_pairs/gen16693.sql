SELECT t.id, t.amount FROM transactions t WHERE t.merchant != 'Amazon' AND t.account_id IN (SELECT id FROM accounts WHERE account_type != 'CHECKING');
