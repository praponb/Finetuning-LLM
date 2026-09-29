SELECT *
FROM (
    SELECT
        id,
        account_id,
        amount,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY amount DESC, id) AS row_num
    FROM transactions
) AS ranked
WHERE row_num <= 2
ORDER BY 1 DESC, 4 DESC;
