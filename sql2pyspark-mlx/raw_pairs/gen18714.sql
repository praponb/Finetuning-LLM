SELECT is_flagged, MAX(amount) AS max_amount FROM transactions WHERE txn_type IN ('FEE', 'DEBIT') GROUP BY is_flagged
