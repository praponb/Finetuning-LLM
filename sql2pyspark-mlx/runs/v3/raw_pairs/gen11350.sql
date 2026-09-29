SELECT id, order_id, paid_at FROM (SELECT id, order_id, paid_at, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY paid_at, id) AS rn FROM payments) AS t WHERE rn <= 3 ORDER BY order_id;
