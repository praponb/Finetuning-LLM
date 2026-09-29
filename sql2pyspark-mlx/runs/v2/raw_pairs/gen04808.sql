SELECT
    orders.customer_id,
    orders.amount,
    stores.region,
    stores.opened_date,
    customers.segment,
    customers.city
FROM orders
INNER JOIN stores ON orders.store_id = stores.id
INNER JOIN customers ON orders.customer_id = customers.id
WHERE orders.order_date < '2025-01-24' OR stores.opened_date <= '2024-06-27'
