SELECT
    id,
    channel,
    shipping_fee,
    amount
FROM orders
WHERE (status = 'PENDING' AND store_id = 18) OR channel IN ('mobile', 'store', 'partner');
