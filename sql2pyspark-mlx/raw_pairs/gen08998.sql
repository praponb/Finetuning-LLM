SELECT
    id,
    account_id,
    amount,
    is_flagged
FROM (
    SELECT
        id,
        account_id,
        amount,
        is_flagged,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY amount, id) AS rnk
    FROM transactions
) AS t
WHERE rnk <= 3
ORDER BY id DESC, 3 DESC
