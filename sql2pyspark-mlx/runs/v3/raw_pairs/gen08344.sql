WITH numbered AS (SELECT id, customer_id, order_date, status, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date, id) AS rn FROM orders) SELECT * FROM numbered WHERE rn <= 2;
