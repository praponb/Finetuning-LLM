SELECT
    t.is_flagged,
    t.merchant,
    a.balance,
    c.name
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
LEFT JOIN customers c ON c.id = a.customer_id
WHERE t.amount >= 1100 OR t.txn_type NOT IN ('FEE', 'REFUND', 'DEBIT');
