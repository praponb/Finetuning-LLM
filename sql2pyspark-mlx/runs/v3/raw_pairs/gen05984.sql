SELECT id, status, paid_at FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY paid_at, id) AS rnk FROM payments WHERE order_id IS NULL) AS ranked WHERE rnk <= 2;
