SELECT *
FROM (
    SELECT account_id, MIN(amount) AS min_amount
    FROM transactions
    GROUP BY account_id
) AS agg
WHERE min_amount < 3900
ORDER BY min_amount DESC, account_id
LIMIT 1;
