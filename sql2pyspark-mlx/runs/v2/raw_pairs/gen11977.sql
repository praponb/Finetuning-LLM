SELECT * FROM (SELECT id, channel, amount, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY amount, id) AS row_num FROM orders) AS ranked WHERE row_num <= 2;
