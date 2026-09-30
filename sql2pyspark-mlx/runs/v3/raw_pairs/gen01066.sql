select id, status, date_sub(end_date, 18) as end_date_minus_18d from subscriptions where monthly_fee < 80 order by status desc;
