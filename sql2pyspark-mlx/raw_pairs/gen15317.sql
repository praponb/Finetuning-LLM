SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, method ORDER BY amount NULLS FIRST, id) AS rn FROM payments QUALIFY rn = 1;
