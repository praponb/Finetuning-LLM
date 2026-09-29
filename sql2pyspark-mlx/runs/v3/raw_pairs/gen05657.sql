SELECT id, store_id, LAST_DAY(order_date) AS order_date_month_end
FROM orders
WHERE amount >= 560;
