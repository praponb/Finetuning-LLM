SELECT
    id,
    channel,
    amount,
    customer_id,
    store_id,
    order_date,
    discount,
    LAST_VALUE(customer_id) OVER (PARTITION BY channel ORDER BY amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_customer_id,
    discount - LAG(discount, 1) OVER (PARTITION BY store_id ORDER BY order_date, id) AS discount_change
FROM orders
WHERE shipping_fee > 24
