SELECT
    id,
    discount,
    customer_id,
    shipping_fee,
    ROW_NUMBER() OVER (ORDER BY discount, id) AS discount_row,
    AVG(shipping_fee) OVER (PARTITION BY customer_id) AS customer_id_avg_shipping_fee
FROM orders
WHERE status = 'CANCELLED'
