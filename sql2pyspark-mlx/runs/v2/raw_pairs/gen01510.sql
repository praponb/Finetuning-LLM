SELECT id, plan, DATE_FORMAT(start_date, 'yyyy-MM') AS start_date_label
FROM subscriptions;
