SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY opened_date DESC, id) rnk
    FROM accounts
) r
WHERE rnk <= 2;
