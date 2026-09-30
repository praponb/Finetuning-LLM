SELECT id, account_type, opened_date
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY opened_date DESC, id) AS rnk
    FROM accounts
    WHERE customer_id IS NULL
) AS r
WHERE rnk <= 3;
