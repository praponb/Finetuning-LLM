SELECT
    id,
    paid_at,
    amount,
    order_id,
    TRIM(method) AS clean_method
FROM payments;
