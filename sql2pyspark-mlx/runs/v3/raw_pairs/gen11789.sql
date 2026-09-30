SELECT id, customer_id, opened_date
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY opened_date, id) AS rnk
    FROM accounts
) AS t
WHERE rnk <= 2
ORDER BY id;
