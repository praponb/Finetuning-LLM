SELECT id, status, balance, PERCENT_RANK() OVER (PARTITION BY status ORDER BY balance DESC) AS balance_pct_rank FROM accounts;
