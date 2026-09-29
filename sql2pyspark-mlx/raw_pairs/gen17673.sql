select id, paid_at, order_id, row_number() over (partition by to_date(paid_at), order_id order by paid_at desc, id) as paid_at_row from payments order by paid_at desc;
