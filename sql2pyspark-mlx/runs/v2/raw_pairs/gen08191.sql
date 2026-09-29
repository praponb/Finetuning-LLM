SELECT
    id,
    store_id,
    order_date,
    GREATEST(discount, shipping_fee) AS greatest_discount_shipping_fee
FROM orders
WHERE customer_id = 20 AND discount BETWEEN 0.25 AND 0.38;
