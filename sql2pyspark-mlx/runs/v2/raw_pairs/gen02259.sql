SELECT amount, INITCAP(TRIM(merchant)) AS merchant_title, CAST(is_flagged AS INT) AS is_flagged_flag
FROM transactions
ORDER BY merchant_title;
