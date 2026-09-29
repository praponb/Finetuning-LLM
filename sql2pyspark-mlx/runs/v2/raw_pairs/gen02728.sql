SELECT
    id,
    status,
    QUARTER(order_date) AS order_date_quarter,
    LENGTH(status) AS status_length
FROM orders
WHERE discount < 0.32
