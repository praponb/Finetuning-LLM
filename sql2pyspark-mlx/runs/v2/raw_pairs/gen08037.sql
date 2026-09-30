SELECT stores.city, MAX(orders.order_date) AS latest_order_date
FROM orders
FULL OUTER JOIN stores ON stores.id = orders.store_id
RIGHT JOIN customers ON customers.id = orders.customer_id
WHERE orders.channel NOT IN ('mobile', 'store')
GROUP BY stores.city
ORDER BY 2, city
LIMIT 10;
