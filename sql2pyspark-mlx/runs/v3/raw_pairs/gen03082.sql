select launch_date from products union all select start_date from subscriptions where status = 'PAUSED' order by launch_date;
