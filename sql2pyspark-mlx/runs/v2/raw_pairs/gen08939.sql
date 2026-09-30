SELECT
    payments.id,
    payments.amount,
    payments.order_id,
    orders.status,
    orders.shipping_fee,
    stores.city,
    stores.sqft
FROM payments
INNER JOIN orders ON payments.order_id = orders.id
INNER JOIN stores ON stores.id = orders.store_id
WHERE stores.opened_date BETWEEN '2023-05-01' AND '2023-07-22' AND payments.status <> 'SUCCESS'
ORDER BY amount DESC, order_id DESC;
