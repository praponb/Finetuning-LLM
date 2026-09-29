SELECT
    orders.id,
    orders.order_date,
    customers.credit_limit,
    customers.city,
    orders.discount - orders.amount AS discount_minus_amount
FROM orders
JOIN customers ON customers.id = orders.customer_id
WHERE orders.channel IN ('web', 'mobile', 'store') AND UPPER(TRIM(customers.name)) = 'IVY CHEN';
