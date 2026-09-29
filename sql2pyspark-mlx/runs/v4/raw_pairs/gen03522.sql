SELECT store_id, ROUND(AVG(amount), 2) AS avg_amount, SUM(CASE WHEN status = 'SHIPPED' THEN 1 ELSE 0 END) AS shipped_count
FROM orders
WHERE order_date BETWEEN '2024-06-26' AND '2024-09-01'
GROUP BY store_id
HAVING MIN(shipping_fee) > 27
