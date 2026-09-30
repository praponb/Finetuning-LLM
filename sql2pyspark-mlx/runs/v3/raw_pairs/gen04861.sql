SELECT * FROM (SELECT *, DENSE_RANK() OVER (PARTITION BY status ORDER BY balance DESC) AS rnk FROM accounts) AS ranked WHERE rnk <= 3;
