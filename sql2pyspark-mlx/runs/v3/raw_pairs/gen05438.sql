SELECT id, status, opened_date
FROM (
    SELECT
        id,
        status,
        opened_date,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY opened_date, id) AS rnk
    FROM accounts
    WHERE balance <= 68000
) AS t
WHERE rnk <= 3
ORDER BY status, opened_date DESC;
