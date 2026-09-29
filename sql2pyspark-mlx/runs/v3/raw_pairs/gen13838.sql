SELECT
    orders.discount,
    payments.amount,
    payments.paid_at,
    CASE WHEN payments.amount >= 1800 THEN 'High' WHEN payments.amount >= 710 THEN 'Medium' ELSE 'Low' END AS amount_level
FROM orders
JOIN payments ON payments.order_id = orders.id
WHERE orders.status IN ('PENDING', 'RETURNED')
