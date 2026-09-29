select id, start_date, status, date_sub(end_date, 71) as end_date_minus_71d, length(plan) as plan_length from subscriptions order by end_date_minus_71d, id limit 50;
