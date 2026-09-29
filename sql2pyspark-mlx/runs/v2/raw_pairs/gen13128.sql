SELECT status, balance_count FROM (SELECT status, COUNT(balance) AS balance_count FROM accounts GROUP BY status) AS agg WHERE balance_count > 5;
