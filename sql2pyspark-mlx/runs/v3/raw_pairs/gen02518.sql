SELECT *, LAST_DAY(order_date) AS order_date_month_end
FROM orders
WHERE status != 'SHIPPED' AND channel != 'store' AND order_date >= '2025-02-01'
ORDER BY channel;
