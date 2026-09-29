SELECT id, supplier_name, city, ROUND(rating / SUM(rating) OVER (PARTITION BY city), 4) AS rating_share, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) AS seq_no FROM suppliers;
