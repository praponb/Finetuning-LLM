SELECT id, order_id, carrier, shipped_date, status FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY delivered_date DESC, id) AS rn FROM shipments) ranked WHERE rn = 1;
