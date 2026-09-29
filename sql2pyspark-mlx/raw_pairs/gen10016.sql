WITH top_accounts AS (
    SELECT
        id,
        status,
        balance,
        RANK() OVER (PARTITION BY status ORDER BY balance DESC) AS row_num
    FROM accounts
)
SELECT *
FROM top_accounts
WHERE row_num <= 3;
