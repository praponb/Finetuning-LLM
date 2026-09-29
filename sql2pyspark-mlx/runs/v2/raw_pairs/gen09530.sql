SELECT
    payments.id,
    payments.amount,
    payments.method,
    orders.store_id,
    orders.discount,
    stores.region
FROM payments
LEFT JOIN orders ON payments.order_id = orders.id
INNER JOIN stores ON orders.store_id = stores.id;
