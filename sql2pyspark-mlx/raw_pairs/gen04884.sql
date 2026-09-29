WITH numbered AS (
    SELECT
        id,
        status,
        balance,
        opened_date,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY balance DESC, id) AS row_num
    FROM accounts
    WHERE status = 'OPEN'
)
SELECT *
FROM numbered
WHERE row_num <= 2;
