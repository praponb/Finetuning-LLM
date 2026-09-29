SELECT * FROM transactions QUALIFY RANK() OVER (PARTITION BY txn_type ORDER BY amount DESC) = 1;
