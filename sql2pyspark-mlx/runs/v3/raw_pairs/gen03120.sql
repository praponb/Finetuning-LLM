SELECT
    id,
    status,
    amount,
    ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount DESC, id) AS row_num
FROM payments;
