WITH top_accounts AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY balance DESC, id) AS rn FROM accounts) SELECT id, account_type, balance FROM top_accounts WHERE rn <= 3
