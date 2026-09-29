SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY balance DESC, id) AS dedup_rank
FROM accounts
QUALIFY dedup_rank = 1
