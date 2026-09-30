SELECT account_id, amount_bucket, cnt
FROM (
    SELECT account_id, CASE WHEN amount >= 4500 THEN 'high' WHEN amount >= 1200 THEN 'mid' ELSE 'low' END AS amount_bucket, COUNT(*) AS cnt
    FROM transactions
    WHERE txn_time >= '2023-11-25'
    GROUP BY account_id, CASE WHEN amount >= 4500 THEN 'high' WHEN amount >= 1200 THEN 'mid' ELSE 'low' END
) AS sub
WHERE cnt < 9;
