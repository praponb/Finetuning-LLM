SELECT *
FROM (
    SELECT
        id,
        status,
        amount,
        order_date,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount DESC, id) AS row_num
    FROM orders
) AS ranked
WHERE row_num <= 3
