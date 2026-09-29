SELECT
    CASE WHEN amount >= 3300 THEN 'high' WHEN amount >= 1000 THEN 'mid' ELSE 'low' END AS amount_bucket,
    account_id,
    txn_type,
    ROUND((100 * SUM(CASE WHEN merchant = 'Grab' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_grab
FROM transactions
WHERE NOT is_flagged
GROUP BY 1, 2, 3;
