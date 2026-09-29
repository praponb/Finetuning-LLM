select id, paid_at, upper(status) as status_upper from payments where order_id in (20, 33) and hour(paid_at) between 6 and 11 order by id desc limit 10;
