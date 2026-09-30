SELECT
    orders.id,
    orders.order_date,
    orders.amount,
    orders.store_id,
    stores.city,
    stores.opened_date
FROM orders
LEFT OUTER JOIN stores ON orders.store_id = stores.id
WHERE stores.store_name LIKE '%Tow%';
