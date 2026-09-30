SELECT
    id,
    method,
    amount,
    order_id
FROM (
    SELECT
        id,
        method,
        amount,
        order_id,
        ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount, id) AS row_num
    FROM payments
) AS t
WHERE row_num = 1;
