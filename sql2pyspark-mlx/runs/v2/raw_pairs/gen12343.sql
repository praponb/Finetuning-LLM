SELECT
    payments.paid_at,
    payments.method,
    orders.order_date,
    stores.opened_date,
    stores.city,
    DATEDIFF(CURRENT_DATE(), orders.order_date) AS days_since_order_date
FROM payments
LEFT JOIN orders ON payments.order_id = orders.id
INNER JOIN stores ON orders.store_id = stores.id
ORDER BY 6 DESC
