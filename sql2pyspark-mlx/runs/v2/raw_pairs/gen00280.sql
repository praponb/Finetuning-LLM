SELECT id, amount, UPPER(merchant) AS merchant_upper
FROM transactions
WHERE merchant IN ('Grab', 'Starbucks', '7-Eleven') OR is_flagged = FALSE;
