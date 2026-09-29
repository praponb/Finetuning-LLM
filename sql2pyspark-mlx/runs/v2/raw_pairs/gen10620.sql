SELECT *, ADD_MONTHS(order_date, 12) AS order_date_plus_months, TRIM(status) AS clean_status
FROM orders
WHERE amount < 730 OR status IN ('PENDING', 'CANCELLED')
ORDER BY order_date, id
LIMIT 20;
