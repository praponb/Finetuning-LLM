SELECT t.id, t.merchant, t.is_flagged
FROM transactions t
WHERE t.txn_type IN ('REFUND', 'DEBIT', 'CREDIT') AND t.account_id NOT IN (SELECT id FROM accounts WHERE account_type = 'CHECKING');
