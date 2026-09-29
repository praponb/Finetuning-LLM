select id, start_date, plan from subscriptions where start_date = (select min(start_date) from subscriptions);
