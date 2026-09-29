SELECT
    id,
    method,
    amount,
    ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount DESC, id) rn,
    ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount, id) rn_2
FROM payments;
