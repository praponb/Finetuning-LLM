SELECT id, txn_type, merchant, is_flagged FROM transactions QUALIFY ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY amount DESC NULLS LAST, id) = 1
