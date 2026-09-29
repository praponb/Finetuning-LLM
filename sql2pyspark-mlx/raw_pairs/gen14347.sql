SELECT *, CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END AS is_flagged_label, DAYOFWEEK(txn_time) AS txn_time_dayofweek
FROM transactions
WHERE amount > 1300 OR merchant = 'Starbucks';
