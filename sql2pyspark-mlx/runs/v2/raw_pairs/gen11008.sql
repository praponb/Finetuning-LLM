SELECT merchant FROM transactions WHERE is_flagged INTERSECT SELECT merchant FROM transactions WHERE account_id = 18;
