SELECT id, is_flagged FROM transactions WHERE merchant != 'Shell' AND merchant IN ('Shell', 'Amazon') ORDER BY id ASC;
