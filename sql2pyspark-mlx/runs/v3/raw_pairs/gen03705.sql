SELECT * FROM (SELECT *, DENSE_RANK() OVER (PARTITION BY account_id ORDER BY amount) AS row_num FROM transactions WHERE account_id = 3) AS t WHERE row_num = 1;
