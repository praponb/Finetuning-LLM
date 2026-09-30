SELECT
    id,
    order_date,
    customer_id,
    TRIM(status) AS clean_status
FROM orders;
