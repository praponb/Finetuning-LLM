WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY discount DESC, id) AS rnk FROM orders) SELECT id, customer_id, discount, channel FROM ranked WHERE rnk <= 5;
