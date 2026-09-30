SELECT
    id,
    discount,
    store_id,
    amount,
    DATEDIFF(CURRENT_DATE(), order_date) AS days_since_order_date
FROM orders
WHERE status IN ('COMPLETED', 'PENDING')
ORDER BY store_id DESC
