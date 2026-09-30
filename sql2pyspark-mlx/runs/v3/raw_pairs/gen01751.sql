SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY cost DESC, id) AS rn FROM products) AS ranked WHERE rn = 1;
