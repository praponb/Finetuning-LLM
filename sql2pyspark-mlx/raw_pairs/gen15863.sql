SELECT *, CAST(amount AS INT) amount_int FROM transactions WHERE txn_type = 'FEE'
