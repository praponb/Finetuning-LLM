SELECT id, status, plan, end_date, ROW_NUMBER() OVER (PARTITION BY status, plan ORDER BY end_date, id) AS row_num FROM subscriptions
