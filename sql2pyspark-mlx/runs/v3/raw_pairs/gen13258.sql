SELECT id, status, balance
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY balance DESC, id) AS pos
    FROM accounts
) AS r
WHERE pos <= 3;
