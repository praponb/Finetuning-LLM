SELECT *, CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END AS is_flagged_label, LENGTH(merchant) AS merchant_length
FROM transactions
