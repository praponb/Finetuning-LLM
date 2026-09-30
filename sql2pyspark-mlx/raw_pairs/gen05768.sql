SELECT
    id,
    store_id,
    status,
    channel,
    amount,
    discount,
    LEAD(amount) OVER (PARTITION BY status ORDER BY discount, id) AS next_amount,
    LAG(amount) OVER (PARTITION BY status ORDER BY discount, id) AS prev_amount,
    LAG(shipping_fee) OVER (PARTITION BY status ORDER BY discount, id) AS prev_shipping_fee,
    LEAD(shipping_fee) OVER (PARTITION BY status ORDER BY discount, id) AS next_shipping_fee,
    ROW_NUMBER() OVER (PARTITION BY status ORDER BY discount, id) AS seq_no,
    LAG(amount, 1) OVER (ORDER BY order_date, id) AS prev_amount_2
FROM orders
