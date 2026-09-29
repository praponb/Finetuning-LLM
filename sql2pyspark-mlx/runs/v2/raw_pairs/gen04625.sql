SELECT
    id,
    status,
    ROUND(shipping_fee, 1) AS shipping_fee_rounded,
    ADD_MONTHS(order_date, 3) AS order_date_plus_months
FROM orders
WHERE order_date <= '2023-08-21'
ORDER BY order_date_plus_months;
