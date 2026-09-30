SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY balance DESC, id) AS pos FROM accounts) AS ranked WHERE pos = 1;
