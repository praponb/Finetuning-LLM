SELECT *, INITCAP(TRIM(merchant)) AS merchant_title, CASE WHEN merchant = 'Shell' THEN 1 ELSE 0 END AS is_shell FROM transactions WHERE amount IS NOT NULL AND amount BETWEEN 2600 AND 3800;
