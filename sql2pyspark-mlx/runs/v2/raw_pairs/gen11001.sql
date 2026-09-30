SELECT store_id, MAX(order_date) AS latest_order_date FROM orders WHERE amount <= 1380 OR status NOT IN ('SHIPPED', 'RETURNED', 'CANCELLED') GROUP BY store_id
