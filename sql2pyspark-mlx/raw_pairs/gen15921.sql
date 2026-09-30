SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY shipped_date DESC, id) AS rank_in_group FROM shipments) t WHERE rank_in_group = 1;
