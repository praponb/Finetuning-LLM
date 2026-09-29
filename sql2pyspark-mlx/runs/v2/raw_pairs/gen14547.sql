SELECT id, plan, start_date, PERCENT_RANK() OVER (PARTITION BY plan ORDER BY start_date) start_date_pct_rank FROM subscriptions ORDER BY start_date_pct_rank DESC
