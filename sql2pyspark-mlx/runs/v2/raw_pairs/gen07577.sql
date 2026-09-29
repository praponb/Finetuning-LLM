WITH numbered AS (
    SELECT
        id,
        status,
        opened_date,
        RANK() OVER (PARTITION BY status ORDER BY opened_date) AS pos
    FROM accounts
)
SELECT *
FROM numbered
WHERE pos = 1
ORDER BY opened_date DESC;
