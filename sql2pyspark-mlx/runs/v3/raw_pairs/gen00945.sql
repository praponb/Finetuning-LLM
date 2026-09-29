SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating, id) row_num FROM suppliers) t WHERE row_num <= 3 ORDER BY id DESC
