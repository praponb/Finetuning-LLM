SELECT id, order_id, method, amount, status FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, status ORDER BY amount DESC, id) AS rn FROM payments) t WHERE rn = 1
