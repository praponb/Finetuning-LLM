SELECT id, balance, DENSE_RANK() OVER (ORDER BY balance) AS balance_dense_rank
FROM accounts;
