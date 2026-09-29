select month(paid_at) as paid_month, method, to_date(paid_at) as paid_day, count(*) as cnt from payments group by 1, 2, 3 having cnt < 4;
