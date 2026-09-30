WITH numbered AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY method ORDER BY paid_at DESC, id) AS rnk FROM payments) SELECT id, method, paid_at FROM numbered WHERE rnk <= 3 ORDER BY method;
