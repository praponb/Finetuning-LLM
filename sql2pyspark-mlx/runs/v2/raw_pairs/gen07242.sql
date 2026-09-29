SELECT id, order_id, amount, status FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount DESC, id) AS rn FROM payments) AS ranked WHERE rn = 1 ORDER BY amount DESC;
