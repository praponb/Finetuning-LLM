SELECT *
FROM (
    SELECT
        store_id,
        status,
        amount
    FROM orders
) src
PIVOT (
    SUM(amount) AS total
    FOR status IN ('CANCELLED' AS cancelled, 'RETURNED' AS returned)
);
