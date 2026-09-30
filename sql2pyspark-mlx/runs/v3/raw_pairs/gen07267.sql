SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY delivered_date DESC, id) AS row_num FROM shipments) AS t WHERE row_num = 1;
