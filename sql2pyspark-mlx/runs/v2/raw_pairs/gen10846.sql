SELECT account_id, is_flagged, amount
FROM transactions
WHERE txn_time < '2023-06-04' AND merchant = 'Grab'
ORDER BY account_id;
