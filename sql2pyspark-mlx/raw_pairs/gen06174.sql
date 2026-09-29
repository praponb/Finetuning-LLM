SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY monthly_fee DESC, id) AS dedup_rank FROM subscriptions QUALIFY dedup_rank = 1;
