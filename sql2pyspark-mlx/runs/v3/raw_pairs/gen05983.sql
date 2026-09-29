SELECT id, plan, start_date, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY start_date DESC, id) AS row_num FROM subscriptions WHERE end_date > '2024-04-01';
