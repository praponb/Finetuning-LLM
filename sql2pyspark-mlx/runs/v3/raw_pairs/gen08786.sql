SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY balance, id) AS rnk
    FROM accounts
    WHERE account_type IN ('CHECKING', 'SAVINGS')
) AS ranked
WHERE rnk = 1;
