SELECT
    id,
    status,
    opened_date,
    NTILE(2) OVER (PARTITION BY status ORDER BY opened_date, id) AS bucket
FROM accounts;
