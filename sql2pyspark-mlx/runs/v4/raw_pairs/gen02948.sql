WITH ranked AS (SELECT id, order_id, paid_at, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY paid_at DESC, id) AS rnk FROM payments) SELECT * FROM ranked WHERE rnk <= 1 ORDER BY rnk;
