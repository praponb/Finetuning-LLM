SELECT id, is_flagged
FROM transactions
WHERE merchant IN ('Grab', 'Shell', 'Lazada')
