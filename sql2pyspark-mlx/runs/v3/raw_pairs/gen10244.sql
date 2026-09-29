select id, order_id, status from payments where amount < 480 union all select id, order_id, status from payments where method = 'card' order by id;
