SELECT
    payments.id,
    payments.amount,
    orders.discount,
    orders.order_date
FROM payments
LEFT JOIN orders ON payments.order_id = orders.id
ORDER BY id, payments.amount;
