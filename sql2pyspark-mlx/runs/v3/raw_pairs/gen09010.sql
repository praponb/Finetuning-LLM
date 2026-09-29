SELECT id, txn_type FROM transactions WHERE (merchant IN ('Shell', 'Starbucks') AND amount >= 2400) OR merchant = 'Lazada'
