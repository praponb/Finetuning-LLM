SELECT id, segment, credit_limit FROM (SELECT id, segment, credit_limit, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY credit_limit, id) AS row_num FROM customers) AS t WHERE row_num <= 3
