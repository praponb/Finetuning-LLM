WITH top_accounts AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY balance DESC, id) AS pos FROM accounts) SELECT * FROM top_accounts WHERE pos <= 2
