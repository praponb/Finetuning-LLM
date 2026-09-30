SELECT
    id,
    customer_id,
    order_date,
    status,
    channel,
    amount,
    shipping_fee
FROM orders
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount, id) = 1;
