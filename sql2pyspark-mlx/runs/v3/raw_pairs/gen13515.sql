SELECT id, store_id, order_date, amount, SUM(amount) OVER (PARTITION BY store_id ORDER BY order_date, id) AS running_amount FROM orders WHERE amount < 970
