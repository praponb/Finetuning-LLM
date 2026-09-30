SELECT id, start_date, ROW_NUMBER() OVER (ORDER BY start_date, id) AS start_date_row
FROM subscriptions
