SELECT id, merchant FROM transactions EXCEPT SELECT id, merchant FROM transactions WHERE merchant IN ('Starbucks', 'Lazada');
