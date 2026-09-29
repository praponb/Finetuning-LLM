SELECT merchant FROM transactions WHERE is_flagged = TRUE UNION SELECT device FROM sessions ORDER BY merchant
