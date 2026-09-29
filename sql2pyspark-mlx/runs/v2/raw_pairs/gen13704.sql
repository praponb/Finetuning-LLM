SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY balance DESC, id) AS pos
    FROM accounts
) AS t
WHERE pos <= 1;
