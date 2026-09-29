SELECT id, method, amount FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount DESC, id) AS rn FROM payments WHERE order_id = 36) AS ranked WHERE rn <= 3;
