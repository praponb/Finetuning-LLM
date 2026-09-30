SELECT merchant FROM transactions INTERSECT SELECT merchant FROM transactions WHERE is_flagged;
