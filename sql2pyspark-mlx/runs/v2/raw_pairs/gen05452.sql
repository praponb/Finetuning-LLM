SELECT id, account_id, COALESCE(amount, 0) AS amount_filled
FROM transactions
WHERE txn_time >= '2023-10-01' AND is_flagged
ORDER BY account_id DESC;
