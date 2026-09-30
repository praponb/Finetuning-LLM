SELECT txn_type, merchant
FROM transactions
WHERE txn_type = 'FEE'
UNION
SELECT txn_type, merchant
FROM transactions
WHERE txn_type = 'CREDIT'
ORDER BY merchant;
