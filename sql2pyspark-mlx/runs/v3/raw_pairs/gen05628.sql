WITH numbered AS (SELECT id, order_id, amount, DENSE_RANK() OVER (PARTITION BY order_id ORDER BY amount DESC) AS rn FROM payments) SELECT * FROM numbered WHERE rn <= 5;
