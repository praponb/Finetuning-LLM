SELECT
    id,
    amount,
    shipping_fee,
    INITCAP(TRIM(status)) AS status_title
FROM orders
WHERE order_date < '2023-03-06' AND shipping_fee <= 34
ORDER BY shipping_fee
