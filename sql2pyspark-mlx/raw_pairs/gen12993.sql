select id, start_date, status, plan from subscriptions where start_date = (select max(start_date) from subscriptions) order by id
