WITH top_payments AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY paid_at, id) AS rnk FROM payments) SELECT id, order_id, paid_at FROM top_payments WHERE rnk = 1;
