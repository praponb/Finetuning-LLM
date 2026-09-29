SELECT *, CAST(is_flagged AS INT) is_flagged_flag FROM transactions WHERE amount BETWEEN 2300 AND 4000 AND is_flagged
