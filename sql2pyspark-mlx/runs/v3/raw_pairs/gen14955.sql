SELECT id, account_id, COALESCE(merchant, 'N/A') AS merchant_filled
FROM transactions
WHERE account_id = 19 AND amount >= 4000 AND account_id IN (5, 6, 11, 20);
