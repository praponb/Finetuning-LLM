SELECT *
FROM (
    SELECT *, DENSE_RANK() OVER (PARTITION BY txn_type ORDER BY amount DESC) AS rnk
    FROM transactions
) AS r
WHERE rnk <= 5;
