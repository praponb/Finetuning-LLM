SELECT * FROM (SELECT store_id, COUNT(amount) AS amount_count FROM orders WHERE MONTH(order_date) IN (2, 5, 8) GROUP BY store_id) AS x WHERE amount_count > 4;
