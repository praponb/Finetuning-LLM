WITH ranked AS (SELECT id, order_id, paid_at, RANK() OVER (PARTITION BY order_id ORDER BY paid_at DESC) AS rn FROM payments) SELECT * FROM ranked WHERE rn <= 3 ORDER BY id, order_id DESC;
