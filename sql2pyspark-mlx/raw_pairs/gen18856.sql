SELECT id, merchant, amount FROM (SELECT id, merchant, amount, RANK() OVER (PARTITION BY merchant ORDER BY amount DESC) AS row_num FROM transactions) AS r WHERE row_num = 1
