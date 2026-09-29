SELECT id, account_id, amount FROM (SELECT id, account_id, amount, RANK() OVER (PARTITION BY account_id ORDER BY amount DESC) AS pos FROM transactions) AS t WHERE pos <= 2
