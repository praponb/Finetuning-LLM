SELECT id, status, opened_date
FROM (
    SELECT
        id,
        status,
        opened_date,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY opened_date, id) AS rn
    FROM accounts
) AS t
WHERE rn <= 3
ORDER BY id DESC;
