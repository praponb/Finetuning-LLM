SELECT id, store_id, sale_date FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY sale_date DESC, id) AS rnk FROM store_sales WHERE sale_date <= '2025-04-01') AS ranked WHERE rnk = 1
