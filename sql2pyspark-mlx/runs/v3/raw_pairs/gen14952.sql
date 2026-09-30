SELECT
    customer_id,
    discount,
    channel,
    amount - discount AS amount_minus_discount,
    YEAR(order_date) AS order_date_year
FROM orders
WHERE status = 'COMPLETED';
