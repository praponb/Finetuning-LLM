SELECT id, country, rating, supplier_name FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC, id) row_num FROM suppliers) ranked WHERE row_num <= 2;
