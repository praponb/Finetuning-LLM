SELECT id, method, amount, NTILE(3) OVER (PARTITION BY method ORDER BY amount DESC, id) AS bucket FROM payments ORDER BY 1, amount DESC;
