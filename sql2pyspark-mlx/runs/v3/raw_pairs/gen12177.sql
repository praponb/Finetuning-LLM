SELECT *, CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END AS is_flagged_label
FROM transactions
WHERE HOUR(txn_time) BETWEEN 7 AND 12 AND txn_type NOT IN ('FEE', 'REFUND')
ORDER BY txn_time, id
LIMIT 20;
