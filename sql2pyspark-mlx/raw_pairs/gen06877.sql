WITH lines AS (SELECT id, customer_id, created_at, key, v FROM carts LATERAL VIEW OUTER EXPLODE(attributes) x AS key, v) SELECT customer_id, key, v FROM lines ORDER BY customer_id DESC;
