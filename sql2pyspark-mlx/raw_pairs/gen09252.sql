SELECT *, RANK() OVER (PARTITION BY status, customer_id ORDER BY balance DESC NULLS FIRST) AS dedup_rank FROM accounts QUALIFY dedup_rank = 1
