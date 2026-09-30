SELECT merchant, account_id FROM transactions WHERE merchant = 'Lazada' UNION SELECT merchant, account_id FROM transactions WHERE amount > 3400
