SELECT
    id,
    status,
    opened_date,
    balance
FROM (
    SELECT *, RANK() OVER (PARTITION BY status ORDER BY opened_date DESC) AS rnk
    FROM accounts
) AS ranked
WHERE rnk <= 2
ORDER BY balance DESC
