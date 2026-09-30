SELECT merchant, min_amount
FROM (
    SELECT merchant, MIN(amount) AS min_amount
    FROM transactions
    GROUP BY merchant
) AS x
WHERE min_amount >= 3700
ORDER BY merchant DESC, min_amount
LIMIT 50;
