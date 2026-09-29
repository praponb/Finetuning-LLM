SELECT
    id,
    discount,
    order_date,
    discount + amount AS discount_plus_amount
FROM orders;
