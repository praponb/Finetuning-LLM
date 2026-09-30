SELECT * FROM (SELECT id, country, rating, supplier_name, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC, id) AS rnk FROM suppliers) AS ranked WHERE rnk <= 1;
