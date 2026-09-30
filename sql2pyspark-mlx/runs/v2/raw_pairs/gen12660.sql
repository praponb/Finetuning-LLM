SELECT *, CAST(is_flagged AS INT) is_flagged_flag, CAST(amount AS INT) amount_int FROM transactions WHERE amount BETWEEN 1800 AND 3900 AND account_id = 38 ORDER BY account_id DESC
