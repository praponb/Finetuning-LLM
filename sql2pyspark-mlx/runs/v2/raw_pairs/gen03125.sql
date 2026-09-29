SELECT *, ADD_MONTHS(order_date, 6) AS order_date_plus_months, ROUND(discount, 1) AS discount_rounded
FROM orders
WHERE store_id = 23 AND discount > 0.18 AND customer_id = 30
