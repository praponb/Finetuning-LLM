SELECT
    id,
    channel,
    shipping_fee,
    ADD_MONTHS(order_date, 3) AS order_date_plus_months,
    DATE_FORMAT(order_date, 'yyyy-MM') AS order_date_label
FROM orders
