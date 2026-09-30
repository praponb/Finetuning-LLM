SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY shipped_date, id) AS dedup_rank
FROM shipments
QUALIFY dedup_rank = 1;
