SELECT *, CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END AS is_flagged_label, FLOOR(amount) AS amount_whole
FROM transactions
WHERE txn_type <> 'REFUND'
ORDER BY amount_whole DESC, amount, id
LIMIT 50
