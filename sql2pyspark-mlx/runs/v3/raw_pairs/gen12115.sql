WITH numbered AS (
    SELECT
        id,
        status,
        opened_date,
        customer_id,
        RANK() OVER (PARTITION BY status ORDER BY opened_date DESC) AS rn
    FROM accounts
)
SELECT *
FROM numbered
WHERE rn <= 1
ORDER BY rn, opened_date DESC
