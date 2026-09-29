SELECT
    payments.id,
    payments.amount,
    payments.order_id,
    orders.channel,
    orders.amount AS order_amount,
    stores.sqft,
    stores.city
FROM payments
INNER JOIN orders ON payments.order_id = orders.id
INNER JOIN stores ON orders.store_id = stores.id
WHERE payments.method != 'card'
ORDER BY payments.amount DESC;
