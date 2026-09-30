SELECT id, status, balance FROM (SELECT id, status, balance, ROW_NUMBER() OVER (PARTITION BY status ORDER BY balance, id) AS pos FROM accounts) AS r WHERE pos <= 1 ORDER BY status;
