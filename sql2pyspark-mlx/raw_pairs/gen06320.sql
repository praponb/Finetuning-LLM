SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount DESC, id) AS pos FROM transactions) AS t WHERE pos <= 5 ORDER BY account_id
