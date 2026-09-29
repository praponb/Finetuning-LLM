select id, status, end_date, row_number() over (partition by status order by end_date, id) as end_date_row from subscriptions where month(start_date) in (4, 7, 11);
