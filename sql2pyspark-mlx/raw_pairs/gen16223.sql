select id, start_date, end_date, plan from subscriptions where start_date >= (select max(start_date) from subscriptions where year(start_date) >= 2024) order by start_date;
