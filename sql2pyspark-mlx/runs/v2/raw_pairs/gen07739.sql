SELECT
    id,
    store_id,
    amount,
    shipping_fee,
    TRIM(status) AS clean_status
FROM orders
WHERE status = 'COMPLETED' AND shipping_fee < 29
