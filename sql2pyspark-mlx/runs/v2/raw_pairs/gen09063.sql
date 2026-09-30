SELECT
    id,
    status,
    amount,
    customer_id,
    CASE WHEN status IN ('RETURNED', 'PENDING') THEN 'Priority' ELSE 'Other' END AS status_group
FROM orders
WHERE MONTH(order_date) IN (1, 6, 11)
