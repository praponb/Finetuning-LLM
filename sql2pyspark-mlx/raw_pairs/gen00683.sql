SELECT *, ROW_NUMBER() OVER (PARTITION BY status, store_id ORDER BY order_date, id) AS rn FROM orders QUALIFY rn = 1;
