WITH top_accounts AS (
    SELECT
        id,
        status,
        opened_date,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY opened_date, id) AS pos
    FROM accounts
    WHERE balance >= 37000
)
SELECT *
FROM top_accounts
WHERE pos <= 3
ORDER BY opened_date
