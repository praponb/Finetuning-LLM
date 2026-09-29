SELECT *, CAST(is_flagged AS INT) AS is_flagged_flag, UPPER(merchant) AS merchant_upper
FROM transactions
WHERE merchant = '7-Eleven' AND is_flagged = TRUE
