SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY amount DESC, id) AS rn FROM transactions) ranked WHERE rn = 1;
