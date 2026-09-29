SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY end_date, id) AS dedup_rank FROM subscriptions QUALIFY dedup_rank = 1
