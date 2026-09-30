WITH top_payments AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount DESC, id) AS pos FROM payments WHERE amount > 1210) SELECT * FROM top_payments WHERE pos <= 2;
