SELECT
    id,
    order_date,
    amount,
    store_id,
    CASE WHEN status IN ('COMPLETED', 'SHIPPED') THEN 'Core' ELSE 'Other' END AS status_group
FROM orders
WHERE store_id = 19 AND (YEAR(order_date) = 2023 OR customer_id IN (10, 22, 38))
ORDER BY store_id DESC, id
LIMIT 50
