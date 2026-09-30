SELECT
    id,
    status,
    order_date,
    customer_id,
    INITCAP(TRIM(status)) AS status_title,
    DATE_TRUNC('month', order_date) AS order_date_period
FROM orders
WHERE order_date < '2023-07-01'
