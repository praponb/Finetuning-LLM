select id, paid_at, amount, order_id, cast(paid_at as date) as paid_at_day from payments order by paid_at_day asc, id asc limit 5;
