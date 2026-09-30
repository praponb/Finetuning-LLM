SELECT *, ROW_NUMBER() OVER (PARTITION BY browser, customer_id ORDER BY started_at DESC, id) AS dedup_rank FROM sessions QUALIFY dedup_rank = 1
