SELECT * FROM (SELECT *, RANK() OVER (PARTITION BY txn_type ORDER BY amount DESC) AS rnk FROM transactions) AS ranked WHERE rnk <= 1;
