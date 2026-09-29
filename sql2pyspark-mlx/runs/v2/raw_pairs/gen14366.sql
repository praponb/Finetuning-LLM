SELECT txn_type, AVG(amount) avg_amount, SUM(CASE WHEN merchant = 'Grab' THEN amount ELSE 0 END) grab_amount
FROM transactions
WHERE NOT (amount >= 1600 AND is_flagged)
GROUP BY txn_type
ORDER BY grab_amount DESC;
