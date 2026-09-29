SELECT id, store_id, sale_date FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY sale_date DESC, id) AS rnk FROM store_sales) AS t WHERE rnk <= 3 ORDER BY id DESC
