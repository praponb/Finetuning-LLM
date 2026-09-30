SELECT QUARTER(order_date) AS order_quarter, COUNT(*) AS order_count
FROM orders
WHERE discount > 0.26
GROUP BY 1
HAVING COUNT(DISTINCT shipping_fee) < 5
ORDER BY order_count;
