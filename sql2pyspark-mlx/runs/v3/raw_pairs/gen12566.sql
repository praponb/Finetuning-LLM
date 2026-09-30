SELECT
    id,
    discount,
    QUARTER(order_date) AS order_date_quarter,
    CEIL(discount) AS discount_whole
FROM orders
WHERE discount <= 0.14
ORDER BY discount_whole, order_date_quarter
