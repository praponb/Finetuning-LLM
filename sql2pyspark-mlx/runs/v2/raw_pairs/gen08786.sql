SELECT *
FROM (
    SELECT
        id,
        status,
        amount,
        store_id,
        RANK() OVER (PARTITION BY status ORDER BY amount) AS rnk
    FROM orders
) AS r
WHERE rnk <= 5
ORDER BY rnk DESC
