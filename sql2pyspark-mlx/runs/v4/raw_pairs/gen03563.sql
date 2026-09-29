SELECT store_id, SUM(amount) AS total_amount, DENSE_RANK() OVER (ORDER BY SUM(amount) DESC) AS amount_rank FROM orders WHERE order_date < '2025-07-07' GROUP BY store_id ORDER BY store_id;
