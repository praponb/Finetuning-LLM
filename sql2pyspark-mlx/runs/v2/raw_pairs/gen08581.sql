WITH numbered AS (SELECT id, segment, credit_limit, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY credit_limit DESC, id) AS pos FROM customers) SELECT * FROM numbered WHERE pos = 1;
