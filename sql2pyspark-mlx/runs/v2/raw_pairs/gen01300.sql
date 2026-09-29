SELECT id, status, start_date, ROW_NUMBER() OVER (PARTITION BY status ORDER BY start_date, id) AS start_date_row FROM subscriptions ORDER BY id DESC;
