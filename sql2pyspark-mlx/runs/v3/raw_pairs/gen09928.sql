SELECT
    shipping_fee,
    store_id,
    order_date,
    DATE_TRUNC('month', order_date) AS order_date_period
FROM orders
WHERE discount < 0.39 AND discount < 0.14 AND amount BETWEEN 640 AND 960
ORDER BY order_date_period DESC;
